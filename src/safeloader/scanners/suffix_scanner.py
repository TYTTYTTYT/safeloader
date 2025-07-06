from pathlib import Path
from typing import List, Union, Optional

from ..bases import Row
from .scanner import Scanner


def find_suffix_paths(
    root: Union[str, Path],
    extensions: set[str],
    max_depth: Optional[int] = None
) -> List[Path]:
    """
    Recursively find all files with specified suffixes under the given root directory, up to a specified recursion depth.

    Args:
        root:        Directory to search (as a str or Path).
        extensions:  Set of lowercase extensions to match (including the leading dot).
        max_depth:   Maximum directory-depth to recurse (0 = only root, 1 = root + its immediate subdirs,
                     None = unlimited).

    Returns:
        List of Path objects for each suffix file found.
    """
    root_path = Path(root)

    if max_depth is not None and max_depth < 0:
        raise ValueError(f"max_depth must be non-negative or None, got {max_depth!r}")

    results: List[Path] = []

    def _recurse(current: Path, depth: int) -> None:
        # If we've gone deeper than allowed, stop.
        if max_depth is not None and depth > max_depth:
            return

        for entry in current.iterdir():
            if entry.is_file() and entry.suffix[1:].upper() in extensions:
                results.append(entry)
            elif entry.is_dir():
                # Only recurse further if we haven't hit max_depth
                _recurse(entry, depth + 1)

    _recurse(root_path, 0)
    return results


class SuffixScanner(Scanner):
    """
    SuffixFolderScanner is a scanner for suffix folders.
    It provides the basic interface for scanning a folder containing suffix files.
    """

    def __init__(self, path: str, **kwargs) -> None:
        super().__init__(path, **kwargs)
        self.paths = sorted(find_suffix_paths(path, **kwargs))

    def __len__(self) -> int:
        """
        Returns the number of suffix files found in the folder.
        """
        return len(self.paths)

    def __getitem__(self, idx: int) -> Row:
        if idx < 0 or idx >= len(self):
            raise IndexError(f'Index {idx} out of range for suffix folder {self.path}')
        return {'path': str(self.paths[idx].absolute())}
