import os, sys

def gather_stats(root):
    folder_count = 0
    file_count = 0
    total_file_size = 0

    for dirname, dirs, files in os.walk(root):
        folder_count += 1
        print(f"Processed {folder_count} folders.", end="\r", flush=True)
        for name in files:
            file_path = os.path.join(dirname, name)
            if os.path.isfile(file_path):
                file_count += 1
                total_file_size += os.path.getsize(file_path)
    print()
    average_file_size = total_file_size / file_count if file_count > 0 else 0
    average_files_per_folder = file_count / folder_count if folder_count > 0 else 0

    print("Overall folder structure:")
    print(f"Root folder: {root}")
    print(f"Number of folders: {folder_count}")
    print(f"Number of files: {file_count}")
    print(f"Average file size: {average_file_size/1024**2:.3f} MiB")
    print(f"Average number of files per folder: {average_files_per_folder:.2f}")
    print(f"Total size of all files: {total_file_size/1024**2:.3f} MiB")
    print(f"Total files: {file_count}")

print(f"Gathering stats for folder: {sys.argv[1]}...")
gather_stats(sys.argv[1])