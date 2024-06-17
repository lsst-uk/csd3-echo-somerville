import os, sys

def gather_stats(root):
    """
    Gather statistics about a directory tree.

    Args:
        root (str): The root directory to start gathering statistics from.

    Returns:
        None

    Prints:
        Overall folder structure:
        - Number of folders
        - Number of files
        - Average file size in bytes
        - Average number of files per folder
    """
    folder_count = 0
    file_count = 0
    total_file_size = 0

    def visit_func(arg, dirname, names):
        """
        Function to visit each file and folder in a directory and update statistics.

        Args:
            arg: Additional argument (not used in this function).
            dirname: The name of the directory being visited.
            names: List of names of files and subdirectories in the directory being visited.

        Returns:
            None
        """
        nonlocal folder_count, file_count, total_file_size

        folder_count += 1

        for name in names:
            file_path = os.path.join(dirname, name)
            if os.path.isfile(file_path):
                file_count += 1
                total_file_size += os.path.getsize(file_path)

    os.path.walk(root, visit_func, None)

    average_file_size = total_file_size / file_count if file_count > 0 else 0
    average_files_per_folder = file_count / folder_count if folder_count > 0 else 0

    print("Overall folder structure:")
    print(f"Root folder: {root}")
    print(f"Number of folders: {folder_count}")
    print(f"Number of files: {file_count}")
    print(f"Average file size: {average_file_size:.2f} bytes")
    print(f"Average number of files per folder: {average_files_per_folder:.2f}")

# Replace '/path/to/folder' with the actual path to the folder you want to analyze
gather_stats(sys.argv[1])