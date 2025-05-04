import os
from typing import List
import logging

logger = logging.getLogger(__name__)

def split_by_partition(list_len: int, partition_num: int, partition_id: int, allow_repeat: bool=False) -> List[int]:
    if partition_id >= partition_num:
        raise KeyError(f'paritition_id {partition_id} exceeds the range of partition num {partition_num}!')
    ids = list(range(list_len))
    if partition_num > list_len:
        if not allow_repeat:
            if partition_id >= list_len:
                return []
            return [partition_id]
        logger.warning(f'Number of list items: {list_len} is less than the number of partitions: {partition_num}, '
                       'please decrease the number of partitions to avoid repeated data')
        left = partition_num - list_len
        logger.warning(f'{left} piece of data are repeated to satisify the partition num')
        rep = left // list_len
        res = left % list_len
        ids += ids * rep + ids[:res]

    inc = len(ids) // partition_num

    if partition_id == partition_num - 1:
        return ids[inc * partition_id:]

    return ids[inc * partition_id: inc * (partition_id + 1)]

def list_all_files(folder_path: str, max_depth: int) -> List[str]:
    """
    Recursively get all file paths under a folder up to a specified depth.

    Args:
    - folder_path (str): The folder to start searching from.
    - max_depth (int): The maximum depth to recurse into subfolders.

    Returns:
    - List[str]: A list of full file paths under the folder.
    """
    all_paths = []

    def recursive_walk(current_path: str, current_depth: int):
        if current_depth > max_depth:
            return

        # Iterate over files and directories in the current directory
        for root, dirs, files in os.walk(current_path):
            # Only process subdirectories if the current depth is within limit
            if current_depth < max_depth:
                dirs[:] = dirs  # Continue searching in subdirectories

            # Add file paths to the list
            for file in files:
                full_path = os.path.join(root, file)
                all_paths.append(full_path)

            # Recursively walk subdirectories if current depth is within limit
            if current_depth < max_depth:
                for dir_name in dirs:
                    recursive_walk(os.path.join(root, dir_name), current_depth + 1)

    # Start the recursive walking
    recursive_walk(folder_path, 0)

    return all_paths
