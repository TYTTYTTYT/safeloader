from typing import Iterator
import os

import pandas as pd

from .types import Row
from .item_reader import ItemReader
from ..utils import split_by_partition, list_all_files


class CSVReader(ItemReader):

    def iter_single_file(self) -> Iterator[Row]:
        items = getattr(self, 'items', None)
        if items is None:
            data = pd.read_csv(self.path)
            items = data.to_dict(orient='records')
            self.items = items

        idxes = split_by_partition(len(items), self.partition_num, self.partition_idx)
        self.rand().shuffle(idxes) if self.shuffle else ...
        self.bidx = (self.begin_idx % len(items)) // self.partition_num

        def iterate() -> Iterator[Row]:
            while True:
                if self.bidx >= len(idxes):
                    break
                idx = idxes[self.bidx]
                yield items[idx]
                self.bidx += 1
            self.epoch += 1
            return

        return iterate()

    def iter_dir(self) -> Iterator[Row]:
        files = list_all_files(self.path, 0)
        files.sort()

        idxes = split_by_partition(len(files), self.partition_num, self.partition_idx)
        self.rand().shuffle(idxes) if self.shuffle else ...
        if self.begin_idx > 0:
            self.rand().shuffle(idxes)  # TODO: how to start with the next file
        self.fidx = 0

        def iterate() -> Iterator[Row]:
            while True:
                if self.fidx > len(idxes):
                    break
                items = pd.read_csv(files[idxes[self.fidx]]).to_dict(orient='records')
                for item in items:
                    yield item
                self.fidx += 1
            self.epoch += 1
            return

        return iterate()

    def __iter__(self) -> Iterator[Row]:
        if os.path.isfile(self.path):
            return self.iter_single_file()
        elif os.path.isdir(self.path) or os.path.islink(self.path) or os.path.ismount(self.path):
            return self.iter_dir()
        else:
            raise ValueError(f'Can not recognize path: {self.path}')
