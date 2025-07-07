from ..bases import Row
from .scanner import Scanner


class JSONScanner(Scanner):
    """
    JSONScanner is a scanner for JSON files.
    It provides the basic interface for scanning a single file.
    """

    def __init__(self, path: str, **kwargs) -> None:
        super().__init__(path, **kwargs)
        import pandas as pd
        self.json = pd.read_json(path, **kwargs)

    def __len__(self) -> int:
        """
        Returns the number of rows in the JSON file.
        """
        return len(self.json)

    def __getitem__(self, idx: int) -> Row:
        if idx < 0 or idx >= len(self):
            raise IndexError(f'Index {idx} out of range for JSON file {self.path}')
        return self.json.iloc[idx].to_dict()

    @classmethod
    def check_file(cls, path: str) -> bool:
        """
        Checks if the file at the given path is a JSON file.
        This method checks the file extension to determine compatibility.
        """
        return path.lower().endswith('.json')
