from ..bases import Row
from .scanner import Scanner


class TSVScanner(Scanner):
    """
    TSVScanner is a scanner for TSV files.
    It provides the basic interface for scanning a single file.
    """

    def __init__(self, path: str, **kwargs) -> None:
        super().__init__(path, **kwargs)
        import pandas as pd
        self.tsv: pd.DataFrame = pd.read_csv(path, sep='\t', **kwargs)

    def __len__(self) -> int:
        """
        Returns the number of rows in the TSV file.
        """
        return len(self.tsv)

    def __getitem__(self, idx: int) -> Row:
        if idx < 0 or idx >= len(self):
            raise IndexError(f'Index {idx} out of range for TSV file {self.path}')
        return self.tsv.iloc[idx].to_dict()
