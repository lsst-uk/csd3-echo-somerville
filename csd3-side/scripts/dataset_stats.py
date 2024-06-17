import os, sys

def gather_stats(root):
    folder_count = 0
    file_count = 0
    total_file_size = 0
    folders_with_mean_filesize_lt_64mb = 0

    for dirname, dirs, files in os.walk(root):
        folder_count += 1
        local_file_count = 0
        print(f"Processed {folder_count} folders.", end="\r", flush=True)
        for name in files:
            file_path = os.path.join(dirname, name)
            if os.path.isfile(file_path):
                local_file_count += 1
                file_count += 1
                total_file_size += os.path.getsize(file_path)
        if local_file_count > 0:
            if total_file_size / 1024**2 / local_file_count < 64:
                folders_with_mean_filesize_lt_64mb += 1
    print()
    average_file_size = total_file_size / file_count if file_count > 0 else 0
    average_files_per_folder = file_count / folder_count if folder_count > 0 else 0

    print("Overall folder structure:")
    print(f"Root folder: {root}")
    print(f"Total number of folders: {folder_count}")
    print(f"Total number of files: {file_count}")
    print(f"Total size of all files: {total_file_size/1024**2:.3f} MiB")
    print(f"Average file size: {average_file_size/1024**2:.3f} MiB")
    print(f"Average number of files per folder: {average_files_per_folder:.2f}")
    print(f"Number of folders with mean file size < 64 MiB: {folders_with_mean_filesize_lt_64mb}")
    print(f"Percentage of folders with mean file size < 64 MiB: {folders_with_mean_filesize_lt_64mb/folder_count*100:.2f}%")
    print(f"Collation of files during backup is advised if the percentage is high (> 10%). Here: {'collate' if folders_with_mean_filesize_lt_64mb/folder_count*100 > 10 else 'do not collate.'}")

print(f"Gathering stats for folder: {sys.argv[1]}...")
gather_stats(sys.argv[1])