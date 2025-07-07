from typing import Iterable, Iterator

from ..bases import ChildrenTrackable, Row, CouldCountable


class Scanner(ChildrenTrackable, Iterable[Row], CouldCountable):
    """
    Scanner is a base class for all scanners.
    It provides the basic interface for scanning a single file.
    """

    def __init__(self, path: str, **kwargs) -> None:
        super().__init__()
        self.path = path
        self.kwargs = kwargs

    def __len__(self) -> int:
        """
        Returns the number of items in the scanner.
        This method should be implemented by subclasses.
        """
        raise NotImplementedError('Please implement this method in children classes!')

    def __getitem__(self, idx: int) -> Row:
        """
        Returns the item at the given index.
        This method should be implemented by subclasses.
        """
        raise NotImplementedError('Please implement this method in children classes!')

    def __iter__(self) -> Iterator[Row]:
        """
        Returns an iterator over the items in the scanner.
        This method should not be overridden by subclasses.
        """
        for idx in range(len(self)):
            yield self[idx]

    @classmethod
    def check_file(cls, path: str) -> bool:
        """
        Checks if the file at the given path is compatible with this scanner.
        This method should be implemented by subclasses to provide file type checking.
        """
        raise NotImplementedError('Please implement this method in children classes!')
