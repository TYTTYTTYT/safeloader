from pathlib import Path

from ..bases import Row
from .scanner import Scanner


class ParquetScanner(Scanner):
    """
    ParquetScanner is a scanner for Parquet files.
    It provides the basic interface for scanning a single file.
    """

    def __init__(self, path: str, **kwargs) -> None:
        super().__init__(path, **kwargs)
        import pyarrow.parquet as pq
        import pandas
        self.parquet: pandas.DataFrame = pq.read_table(path).to_pandas()

    def __len__(self) -> int:
        """
        Returns the number of rows in the Parquet file.
        """
        return len(self.parquet)

    def __getitem__(self, idx: int) -> Row:
        if idx < 0 or idx >= len(self):
            raise IndexError(f'Index {idx} out of range for Parquet file {self.path}')
        return self.parquet.iloc[idx].to_dict()

    @classmethod
    def check_file(cls, path: str) -> bool:
        """
        Checks if the file at the given path is a Parquet file or a directory.
        This method checks the file extension to determine compatibility.
        """
        return path.lower().endswith('.parquet') or path.lower().endswith('.parq') or Path(path).is_dir()
