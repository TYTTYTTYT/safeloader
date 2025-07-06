from ..bases import Row
from .scanner import Scanner


class LineScanner(Scanner):
    """
    LineScanner is a scanner for text files.
    It provides the basic interface for scanning a single file line by line.
    """

    def __init__(self, path: str, **kwargs) -> None:
        super().__init__(path, **kwargs)
        self.file = open(path, 'r', encoding='utf-8')

    def __len__(self) -> int:
        """
        Returns the number of lines in the text file.
        """
        return sum(1 for _ in self.file)

    def __getitem__(self, idx: int) -> Row:
        if idx < 0 or idx >= len(self):
            raise IndexError(f'Index {idx} out of range for text file {self.path}')
        self.file.seek(0)  # Reset file pointer to the beginning
        for i, line in enumerate(self.file):
            if i == idx:
                return {'line': line.strip()}
        raise IndexError(f'Index {idx} out of range for text file {self.path}')
