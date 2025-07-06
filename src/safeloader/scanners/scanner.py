from ..bases import ChildrenTrackable, Row


class Scanner(ChildrenTrackable):
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
