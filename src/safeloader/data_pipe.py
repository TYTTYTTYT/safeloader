from __future__ import annotations
from typing import Iterable, List, Union

from .bases import Row, ChildrenTrackable


class DataPipe(ChildrenTrackable):
    """
    DataPipe is a class that provides a unified interface for data processing.
    It can be used to map data in a map style or pipe style which returns an iterable.
    """

    def map_fn(self, row: Row) -> Row:
        """
        Default map function that returns the row as is.
        This can be overridden by subclasses to provide custom mapping logic.
        """
        return row

    def pipe_fn(self, source: Iterable[Union[Row, Exception]]) -> Iterable[Union[Row, Exception]]:
        """
        Default pipe function that returns the source as is.
        This can be overridden by subclasses to provide custom piping logic.
        """
        def default_pipe(source: Iterable[Union[Row, Exception]]) -> Iterable[Union[Row, Exception]]:
            for row in source:
                if isinstance(row, Exception):
                    yield row
                    continue

                try:
                    yield self.map_fn(row)
                except Exception as e:
                    yield e
        return default_pipe(source)

    def is_countable(self) -> bool:
        """
        Returns True if the DataPipe is countable, meaning it does not change the number of rows.
        This can be overridden by subclasses to indicate stability.
        """
        if self.__class__.pipe_fn is DataPipe.pipe_fn:
            return True
        return False

    def __call__(self, source: Iterable[Union[Row, Exception]]) -> Iterable[Union[Row, Exception]]:
        """
        Default pipe function that returns the source as is.
        This can be overridden by subclasses to provide custom piping logic.
        """
        return self.pipe_fn(source)

    def __or__(self, other: DataPipe) -> DataPipe:
        """
        Allows chaining of DataPipes using the | operator.
        This is useful for creating a pipeline of data processing steps.
        """
        if not isinstance(other, DataPipe):
            raise TypeError(f"Expected DataPipe, got {type(other).__name__}")

        class ChainedDataPipe(DataPipe):
            def __init__(self, pipes: List[DataPipe]) -> None:
                super().__init__()
                self.pipes = pipes

            def pipe_fn(self, source: Iterable[Union[Row, Exception]]) -> Iterable[Union[Row, Exception]]:
                for pipe in self.pipes:
                    source = pipe.pipe_fn(source)
                return source

            def is_countable(self) -> bool:
                return all(pipe.is_countable() for pipe in self.pipes)

        return ChainedDataPipe([self, other])

    def chain(self, other: DataPipe) -> DataPipe:
        """
        Allows chaining of DataPipes using the chain method.
        This is useful for creating a pipeline of data processing steps.
        """
        return self.__or__(other)
