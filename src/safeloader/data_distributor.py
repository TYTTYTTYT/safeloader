from __future__ import annotations
from typing import Iterable, Iterator, Optional
import os
import random
from dataclasses import dataclass
import logging

from numpy import partition

logger  = logging.getLogger(__name__)

try:
    from torch.multiprocessing import Queue
    logger.info("Using torch.multiprocessing.Queue for index queue.")
except ImportError:
    from multiprocessing import Queue
    logger.info("Using multiprocessing.Queue for index queue.")

from .scanners import Scanner
from .bases import Row, ChildrenTrackable, CouldCountable
from .utils import split_by_partition, list_all_files


@dataclass
class DistributorIndex:
    """
    DistributorIndex is a dataclass that represents the index of a data row.
    It contains the partition index, the row index within that partition, and the epoch num.
    This is used to track the location of data rows in a distributed setting.
    """
    partition_index: int
    row_index: int
    epoch_num: int

    def __str__(self) -> str:
        return (f"DistributorIndex(partition_index={self.partition_index}, row_index={self.row_index}, "
                f"epoch_num={self.epoch_num})")


class DataDistributor(ChildrenTrackable, Iterable[Row], CouldCountable):
    """
    DataDistributor is a class that manages the distribution of data across multiple wokers.
    It allows for parallel processing of data by distributing it to different workers based on worker IDs.
    It records the random seed and epoch num for each worker to ensure representable data order.
    """

    def __init__(
        self,
        path: str,
        scanner_type: type[Scanner],
        worker_id: int,
        worker_num: int,
        base_seed: int,
        shuffle: bool = False,
        index_queue: Optional[Queue[DistributorIndex]] = None
    ):
        super().__init__()
        self.path = path
        self.scanner_type = scanner_type
        self.worker_id = worker_id
        self.worker_num = worker_num
        self.base_seed = base_seed
        self.shuffle = shuffle
        self.index_queue = index_queue

        if worker_id < 0 or worker_id >= worker_num:
            raise ValueError(f"worker_id {worker_id} is out of range [0, {worker_num})")
        if worker_num <= 0:
            raise ValueError(f"worker_num must be greater than 0, got {worker_num}")

        self._partition_index = 0
        self._row_index = 0
        self._epoch_num = 0
        self._index = 0

    def seek(self, partition_index: int, row_index: int, epoch_num: int) -> None:
        """
        Seek to a specific partition and row index for the current worker.
        This method updates the internal state of the distributor to reflect the new position.
        """
        self._partition_index = partition_index
        self._row_index = row_index
        self._epoch_num = epoch_num

    @property
    def index(self) -> int:
        """
        Returns the current index of the worker in the data distribution.
        This is used to track the number of rows processed by the worker.
        """
        return self._index

    @property
    def seed(self) -> str:
        """
        Returns the seed for the current worker based on the base seed, worker ID, and epoch number.
        This is used to ensure that the data order is consistent across epochs.
        """
        return f"{self.base_seed}_{self.worker_id}_{self._epoch_num}"

    def __iter__(self) -> Iterator[Row]:
        """
        Returns an iterator over the rows of data distributed to this worker.
        This method should be implemented by subclasses to provide the actual data iteration logic.
        """
        raise NotImplementedError("Iteration is not implemented.")


class IndexableDsitributor(DataDistributor):
    """
    IndexableDataDistributor is a class that manages the distribution of data across multiple workers
    and allows for indexing into the data.
    It extends DataDistributor to provide additional functionality for indexing.
    """

    def _get_scanner(self) -> Scanner:
        if getattr(self, 'scanner', None) is None:
            self.scanner = self.scanner_type(self.path)
        return self.scanner

    def __len__(self) -> int:
        if getattr(self, '_ids', None) is None:
            total_len = len(self._get_scanner())
            logger.info(f"WORKER_{self.worker_id}: found {total_len} rows in {self.path}")
            self.local_ids = split_by_partition(total_len, self.worker_num, self.worker_id, allow_repeat=False)
            logger.info(f"Allocated {len(self.local_ids)} rows to WORKER_{self.worker_id} from {self.path}")
        return len(self.local_ids)

    def __iter__(self) -> Iterator[Row]:
        entries = list(range(len(self)))
        scanner = self._get_scanner()

        if self.shuffle:
            random.Random(self.seed).shuffle(entries)

        logger.info(f"WORKER_{self.worker_id}: the first row is {scanner[self.local_ids[entries[0]]]} ")

        def _iter() -> Iterator[Row]:
            while self._row_index < len(entries):
                entry = entries[self._row_index]
                index = self.local_ids[entry]

                if self.index_queue is not None:
                    self.index_queue.put(DistributorIndex(0, self._row_index, self._epoch_num))

                yield scanner[index]

                self._row_index += 1
                self._index += 1

            self._epoch_num += 1
            self._partition_index = 0
            self._row_index = 0

        return _iter()


class ShardedDistributor(DataDistributor):
    """
    ShardedDistributor is a class that manages the distribution of data across multiple workers
    and allows for sharded datasets.
    """

    def __iter__(self) -> Iterator[Row]:
        if getattr(self, 'local_partitions', None) is None:
            max_folder_depth = int(os.getenv('SAFELOADER_MAX_FOLDER_DEPTH', '5'))
            logger.info(f"WORKER_{self.worker_id}: checking all files in {self.path} with max depth {max_folder_depth}."
                        f" You can set the environment variable SAFELOADER_MAX_FOLDER_DEPTH to change this value.")
            possibles = list_all_files(self.path, int(os.getenv('SAFELOADER_MAX_FOLDER_DEPTH', '5')))

            all_partitions = sorted([p for p in possibles if self.scanner_type.check_file(p)])
            logger.info(f"WORKER_{self.worker_id}: found {len(all_partitions)} partitions in {self.path}.")

            partition_ids = split_by_partition(
                list_len=len(all_partitions),
                partition_num=self.worker_num,
                partition_id=self.worker_id,
                allow_repeat=False
            )
            self.local_partitions = sorted([all_partitions[idx] for idx in partition_ids])
            logger.info(f'Allocated {len(self.local_partitions)} partitions to WORKER_{self.worker_id} from {self.path}.')

        partition_entries = list(range(len(self.local_partitions)))

        seed = self.seed
        if self.shuffle:
            random.Random(seed).shuffle(partition_entries)

        def _iter() -> Iterator[Row]:
            while self._partition_index < len(partition_entries):
                partition_entry = partition_entries[self._partition_index]
                partition_path = self.local_partitions[partition_entry]
                scanner = self.scanner_type(partition_path)
                logger.info(f"WORKER_{self.worker_id}: found {len(scanner)} rows from partition {partition_path}.")
                logger.info(f"WORKER_{self.worker_id}: the frist row is {scanner[0]}.") if self._epoch_num == 0 else ...

                scanner_entries = list(range(len(scanner)))
                if self.shuffle:
                    random.Random(seed).shuffle(scanner_entries)

                while self._row_index < len(scanner_entries):
                    scanner_entry = scanner_entries[self._row_index]

                    if self.index_queue is not None:
                        self.index_queue.put(DistributorIndex(self._partition_index, self._row_index, self._epoch_num))

                    yield scanner[scanner_entry]

                    self._row_index += 1
                    self._index += 1

                self._partition_index += 1
                self._row_index = 0

            self._epoch_num += 1
            self._partition_index = 0

        return _iter()
