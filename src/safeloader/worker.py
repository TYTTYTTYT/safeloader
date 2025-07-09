from __future__ import annotations
from typing import Iterable, Iterator, List, Optional, Callable, Dict, Hashable, Any, Tuple, Union
from dataclasses import dataclass
import time
import logging

logger  = logging.getLogger(__name__)

from pexpect import ExceptionPexpect
from sympy import inverse_sine_transform
import torch
from torch.multiprocessing import Queue, Process
import atexit

from .scanners import Scanner
from .bases import Row, Batch
from .data_distributor import DataDistributor, DistributorIndex, SharedDistributorIndex
from .data_pipe import DataPipe

def _default_collate_fn(rows: List[Row]) -> Batch:
    """
    Default collate function that returns a Batch from a list of Rows.
    This can be overridden by the user to provide custom collation logic.
    """
    batch: Dict[Hashable, List[Any]] = dict()

    for acc, row in enumerate(rows):
        for key, value in row.items():
            if key not in batch:
                batch[key] = []
            if len(batch[key]) < acc:
                for _ in range(acc - len(batch[key])):
                    batch[key].append(None)
            batch[key].append(value)

    result: Batch = {}
    for key, v_lst in batch.items():
        if isinstance(v_lst[0], torch.Tensor):
            try:
                result[key] = torch.stack(v_lst)
            except Exception:
                result[key] = v_lst
        else:
            result[key] = v_lst

    return result

def _worker_loop(
    path: str,
    batch_size: int,
    scanner_type: type[Scanner],
    distributor_type: type[DataDistributor],
    data_pipe_type: type[DataPipe],
    worker_id: int,
    worker_num: int,
    seed: int,
    shuffle: bool,
    index: SharedDistributorIndex,
    initial_index: DistributorIndex,
    result_queue: Queue[Union[Batch, StopIteration]],
    error_queue: Queue[Exception],
    countable_queue: Queue[Tuple[bool, int]],
    collate_fn: Optional[Callable[[List[Row]], Batch]] = None,
    persistent: bool = False,
    drop_last: bool = False,
    qps: float = 0.0
) -> None:
    """
    Worker loop for processing data in a distributed manner.

    Args:
        path (str): The path to the data source.
        scanner_type (type[Scanner]): The type of scanner to use for reading data.
        distributor_type (type[DataDistributor]): The type of distributor to use for distributing data.
        data_pipe_type (type[DataPipe]): The type of data pipe to use for processing data.
        worker_id (int): The ID of the worker.
        worker_num (int): The total number of workers.
        seed (int): The random seed for shuffling data.
        shuffle (bool): Whether to shuffle the data.
        initial_index (DistributorIndex): Initial index for the worker.
        result_queue (Queue[Row]): Queue for storing processed rows.
        persistent (bool): Whether the worker should run persistently or exit after processing all data.
    """
    # Initialize the distributor
    distributor = distributor_type(
        path=path,
        scanner_type=scanner_type,
        worker_id=worker_id,
        worker_num=worker_num,
        base_seed=seed,
        shuffle=shuffle,
        index=index
    )

    # Initialize the data pipe
    data_pipe = data_pipe_type()

    # Set the initial index
    distributor.seek(initial_index.partition_index, initial_index.row_index, initial_index.epoch_num)

    if collate_fn is None:
        collate_fn = _default_collate_fn

    if distributor.is_countable() and data_pipe.is_countable():
        # If both distributor and data pipe are countable, we can calculate the number of batches
        total_rows: int = len(distributor) # type: ignore
        if not drop_last:
            num_batches = (total_rows + batch_size - 1) // batch_size
        else:
            num_batches = total_rows // batch_size
        countable_queue.put((True, num_batches))
    else:
        # If either distributor or data pipe is not countable, we cannot determine the number of batches
        countable_queue.put((False, -1))

    if qps < 0.0:
        raise ValueError(f"qps must be non-negative, got {qps}")
    elif qps > 0.0:
        min_gap = 1.0 / qps * worker_num
    else:
        min_gap = 0.0

    if min_gap > 0.0:
        tic = time.time()
    else:
        tic = None

    current_batch: List[Row] = []
    while True:
        it = iter(data_pipe(distributor))
        while True:
            try:
                if tic is not None:
                    now = time.time()
                    if now - tic < min_gap:
                        time.sleep(min_gap - (now - tic))
                    tic = time.time()

                row = next(it)
                if isinstance(row, Exception):
                    logger.warning(f"WORKER_{worker_id} Error collating batch: {row}")
                    error_queue.put(row)
                    continue

                current_batch.append(row)

                # Check if the batch size is reached
                try:
                    if len(current_batch) >= batch_size:
                        batch = collate_fn(current_batch)

                        result_queue.put(batch)
                        current_batch = []
                except Exception as e:
                    error_queue.put(e)
                    logger.warning(f"WORKER_{worker_id} Error collating batch: {e}")
                    current_batch = []

            except StopIteration as e:
                if not drop_last and len(current_batch) > 0:
                    batch = collate_fn(current_batch)
                    result_queue.put(batch)
                result_queue.put(e)
                break
            except Exception as e:
                error_queue.put(e)
                logger.warning(f"WORKER_{worker_id} Error processing row: {e}")

        if not persistent:
            break


@dataclass
class WorkerItem:
    err_cnt: int
    current_index: DistributorIndex
    batch: Batch
    errors: List[Exception]


class Worker(Iterable[WorkerItem]):
    """
    Worker class for processing data in a distributed manner.
    It initializes the worker loop with the provided parameters and manages the queues for results and errors.
    """

    def __init__(
        self,
        path: str,
        batch_size: int,
        scanner_type: type[Scanner],
        distributor_type: type[DataDistributor],
        worker_id: int,
        worker_num: int,
        seed: int,
        shuffle: bool,
        *,
        data_pipe_type: Optional[type[DataPipe]] = None,
        current_index: Optional[DistributorIndex] = None,
        collate_fn: Optional[Callable[[List[Row]], Batch]] = None,
        persistent: bool = False,
        drop_last: bool = False,
        qps: float = 0.0,
        buffer_size: int = 1,
        timeout: Optional[float] = 10.0,
        restart_cnt: int = 5,
        ignore_errors: bool = False
    ) -> None:
        """
        Initializes the Worker with the given parameters.

        Args:
            path (str): The path to the data source.
            batch_size (int): The size of each batch to process.
            scanner_type (type[Scanner]): The type of scanner to use for reading data.
            distributor_type (type[DataDistributor]): The type of distributor to use for distributing data.
            data_pipe_type (type[DataPipe]): The type of data pipe to use for processing data.
            worker_id (int): The ID of the worker.
            worker_num (int): The total number of workers.
            seed (int): The random seed for shuffling data.
            shuffle (bool): Whether to shuffle the data.
            initial_index (Optional[DistributorIndex]): Initial index for the worker.
            collate_fn (Optional[Callable[[List[Row]], Batch]]): Custom collate function for batching rows.
            persistent (bool): Whether the worker should run persistently or exit after processing all data.
            drop_last (bool): Whether to drop the last incomplete batch if it is smaller than batch_size.
            qps (float): Queries per second rate limit for processing rows.
            buffer_size (int): The size of the result queue.
            timeout (float): timeout seconds while getting the result.
            restart_cnt (int): maximum number of worker restarting while the worker is dead. Restart count will be
                               renewed every time the worker produced a batch.
        """

        if current_index is None:
            current_index = DistributorIndex(0, 0, 0)
        assert buffer_size > 0, f"buffer_size must be greater than 0, got {buffer_size}"

        if data_pipe_type is None:
            data_pipe_type = DataPipe

        self.path = path
        self.batch_size = batch_size
        self.scanner_type = scanner_type
        self.distributor_type = distributor_type
        self.data_pipe_type = data_pipe_type
        self.worker_id = worker_id
        self.worker_num = worker_num
        self.seed = seed
        self.shuffle = shuffle
        self.current_index = current_index
        self.collate_fn = collate_fn
        self.persistent = persistent
        self.drop_last = drop_last
        self.qps = qps
        self.buffer_size = buffer_size
        self.timeout = timeout
        self.restart_cnt = restart_cnt
        self.ignore_errors = ignore_errors

        self._index = None
        self.result_queue = None
        self.error_queue = None
        self.countble_queue = None

        self._worker_process: Optional[Process] = None

        self.start(self.current_index)

    def start(self, initial_index: DistributorIndex) -> None:
        """
        Starts the worker process with the given initial index.
        """
        self._index = SharedDistributorIndex()
        self._index.set(initial_index)
        self.result_queue: Optional[Queue[Union[Batch, StopIteration]]] = Queue(maxsize=self.buffer_size)
        self.error_queue: Optional[Queue[Exception]] = Queue()
        self.countble_queue: Optional[Queue[Tuple[bool, int]]] = Queue(1)

        self._worker_process = torch.multiprocessing.Process(
            target=_worker_loop,
            args=(
                self.path,
                self.batch_size,
                self.scanner_type,
                self.distributor_type,
                self.data_pipe_type,
                self.worker_id,
                self.worker_num,
                self.seed,
                self.shuffle,
                self._index,
                initial_index,
                self.result_queue,
                self.error_queue,
                self.countble_queue,
                self.collate_fn,
                self.persistent,
                self.drop_last,
                self.qps
            )
        )
        self._worker_process.daemon = True
        self._worker_process.start()

        countable = self.countble_queue.get()
        if countable[0] and not self.ignore_errors:
            self._is_countable = True
            self._len = countable[1]
        else:
            self._is_countable = False
            self._len = -1

        atexit.register(self.stop)

    def stop(self) -> None:
        if self._worker_process is not None and self._worker_process.is_alive():
            self._worker_process.terminate()
            self._worker_process.join()
            self._worker_process = None

        if self.result_queue:
            self.result_queue.close()
            self.result_queue = None
        if self.error_queue:
            self.error_queue.close()
            self.error_queue = None
        if self.countble_queue:
            self.countble_queue.close()
            self.countble_queue = None

    def __iter__(self) -> Iterator[WorkerItem]:
        if self._worker_process is None or not self._worker_process.is_alive():
            self.start(self.current_index)

        def _iter() -> Iterator[WorkerItem]:
            assert self.result_queue is not None
            assert self.error_queue is not None
            assert self._index is not None

            while True:
                batch = None
                if self.ignore_errors:
                    for _ in range(self.restart_cnt):
                        try:
                            batch = self.result_queue.get(block=True, timeout=self.timeout)
                            break
                        except Exception as e:
                            logger.warning(f"WORKER_{self.worker_id}: Get batch failed! {e}")
                        index = self._index.get()
                        self.current_index = index
                        self.stop()
                        index.row_index += 1
                        self.start(index)
                    if batch is None:
                        raise RuntimeError(f'WORKER_{self.worker_id}: Getting batch failed {self.restart_cnt} times!')
                else:
                    batch = self.result_queue.get(timeout=self.timeout)

                if isinstance(batch, StopIteration):
                    self.current_index.partition_index = 0
                    self.current_index.row_index = 0
                    self.current_index.epoch_num += 1
                    break

                err_cnt = 0
                errors = []
                if not self.error_queue.empty():
                    if not self.ignore_errors:
                        raise RuntimeError(f'WORKER_{self.worker_id}: Error happened! {self.error_queue.get()}')
                    while not self.error_queue.empty():
                        errors.append(self.error_queue.get())
                        err_cnt += 1

                self.current_index = self._index.get()
                yield WorkerItem(err_cnt, self.current_index, batch, errors)

        return _iter()

    def is_countable(self) -> bool:
        return self._is_countable

    def __len__(self) -> int:
        return self._len
