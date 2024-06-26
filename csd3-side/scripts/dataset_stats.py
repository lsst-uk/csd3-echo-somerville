import os, sys

def gather_stats(root):
    folder_count = 0
    file_count = 0
    total_file_size = 0
    folders_with_mean_filesize_lt_64mb = 0
    folders_with_lt_2_files = 0
    potential_zip_content = 0

    for dirname, dirs, files in os.walk(root):
        folder_count += 1
        local_file_count = 0
        local_file_size = 0

        # print(files)
        # print(dirs)
        print(f"Processed {folder_count} folders.", end="\r", flush=True)
        for name in files:
            file_path = os.path.join(dirname, name)
            if os.path.isfile(file_path):
                file_count += 1
                local_file_count += 1
                total_file_size += os.path.getsize(file_path)
                local_file_size += os.path.getsize(file_path)
        if local_file_count > 0:
            if local_file_count <= 2 and local_file_size / 1024**2 / local_file_count <= 64:# and os.path.abspath(os.path.join(dirname,os.path.pardir)) == os.path.abspath(root):
                potential_zip_content += 1
        # print(local_file_size)
        # print(total_file_size)
        if local_file_count > 0:
            # print(local_file_size / 1024**2 / local_file_count)
            if local_file_size / 1024**2 / local_file_count <= 64:
                folders_with_mean_filesize_lt_64mb += 1
        if local_file_count <= 2:
            folders_with_lt_2_files += 1
    print()
    if file_count > 0:
        average_file_size = total_file_size / file_count
    else:
        average_file_size = 0
    if folder_count > 0:
        average_files_per_folder = file_count / folder_count
    else:
        average_files_per_folder = 0

    suggestion = "collation not advised."
    if folder_count > 0:
        if folders_with_mean_filesize_lt_64mb/folder_count*100 > 10 and potential_zip_content/folder_count*100 > 10:
            suggestion = "collation advised - high proportion of small files and collatable folders."
        elif folders_with_mean_filesize_lt_64mb/folder_count*100 > 5:
            suggestion = "collation advised - high proportion of small files, although potentially low number of collatable folders."
        elif potential_zip_content/folder_count*100 > 5:
            suggestion = "collation advised - high proportion of folders can be collated."

    if folder_count > 0:
        perc_small_files = folders_with_mean_filesize_lt_64mb/folder_count*100
        perc_low_file_count = folders_with_lt_2_files/folder_count*100
    else:
        perc_small_files = 0
        perc_low_file_count = 0

    print("Overall folder structure:")
    print(f"Root folder: {root}")
    print(f"Total number of folders: {folder_count}")
    print(f"Total number of files: {file_count}")
    print(f"Total size of all files: {total_file_size/1024**2:.0f} MiB")
    print(f"Average file size: {average_file_size/1024**2:.0f} MiB")
    print(f"Average number of files per folder: {average_files_per_folder:.0f}")
    print(f"Number of folders with mean file size <= 64 MiB: {folders_with_mean_filesize_lt_64mb}")
    print(f"Number of folders with <= 2 files: {folders_with_lt_2_files}")
    print(f"Percentage of folders with mean file size <= 64 MiB: {perc_small_files:.0f} %")
    print(f"Percentage of folders with <= 2 files: {perc_low_file_count:.0f} %")
    print(f"Potential number of collated folders: {potential_zip_content}")
    print(f"Potential percentage of collated folders: {potential_zip_content/folder_count*100:.0f} %")
    print(f"Collation of files during backup is advised if both percentages are high.\n\u2794  Here: {suggestion}")

print(f"Gathering stats for folder: {sys.argv[1]}...")
gather_stats(sys.argv[1])