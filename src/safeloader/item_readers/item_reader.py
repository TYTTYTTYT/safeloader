from typing import Iterator, Iterable
from random import Random

from .types import Row


class ItemReader(object):

    def __init__(
        self,
        seed: int,
        path: str,
        partition_idx: int,
        partition_num: int,
        begin_idx: int=0,
        begin_epoch: int=0,
        shuffle: bool=False,
        **kwargs
    ) -> None:
        super().__init__()
        self.seed = seed
        self.path = path
        self.partition_idx = partition_idx
        self.partition_num = partition_num
        self.begin_idx = begin_idx
        self.epoch = begin_epoch
        self.shuffle = shuffle

    def __iter__(self) -> Iterator[Row]:
        raise NotImplementedError('Please implement this method in children classes!')

    def rand(self) -> Random:
        rand = getattr(self, '_rand', None)
        if rand is None:
            self._rand = Random(self.seed + self.epoch)
            self.rand_epoch = self.epoch
        elif self.rand_epoch != self.epoch:
            assert self.epoch > self.rand_epoch, 'something is wrong, the epoch should be increasing!'
            self._rand = Random(self.seed + self.epoch)
            self.rand_epoch = self.epoch
        return self._rand
