from pathlib import Path

from ..bases import Row
from .scanner import Scanner


class CSVScanner(Scanner):
    """
    CSVScanner is a scanner for CSV files.
    It provides the basic interface for scanning a single file.
    """

    def __init__(self, path: str, **kwargs) -> None:
        super().__init__(path, **kwargs)
        import pandas as pd
        self.csv = pd.read_csv(path)

    def __len__(self) -> int:
        """
        Returns the number of rows in the CSV file.
        """
        return len(self.csv)

    def __getitem__(self, idx: int) -> Row:
        if idx < 0 or idx >= len(self):
            raise IndexError(f'Index {idx} out of range for CSV file {self.path}')
        return self.csv.iloc[idx].to_dict()

    @classmethod
    def check_file(cls, path: str) -> bool:
        """
        Checks if the file at the given path is a CSV file or a directory.
        This method checks the file extension to determine compatibility.
        """
        return path.lower().endswith('.csv') or Path(path).is_dir()
