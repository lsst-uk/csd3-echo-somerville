import pandas as pd
import os
import sys
import dask.dataframe as dd

def get_local_file_list(local_folder):
    local_files = []
    local_folders = []
    
    for root, dirs, files in os.walk(local_folder):
        print('root:', root)
        print('dirs:', dirs)
        print('files:', files)
        these_local_files = []
        these_local_folders = []
        for file in files:
            these_local_files.append(os.path.join(root, file))
        for folder in dirs:
            these_local_folders.append(os.path.join(root, folder))
        local_files.extend(these_local_files)
        local_folders.extend(these_local_folders)
    return local_files, local_folders

def generate_exclude_list(df, local_folder):
    exclude_list = []
    folder_files = {}

    # Read the CSV file using dask
    for _, row in df.iterrows():
        file_name = os.path.basename(local_folder)

        # Add the file to the folder_files dictionary
        if local_folder not in folder_files:
            folder_files[local_folder] = set()
        folder_files[local_folder].add(file_name)

    # Check if all files in each folder are in the CSV file
    for folder, files in folder_files.items():
        all_files_in_csv = all(file in df.columns for file in files)
        if all_files_in_csv:
            exclude_list.append(folder)

    return exclude_list

# Usage example
csv_file = sys.argv[1]
#LOCAL_FOLDER,LOCAL_PATH,FILE_SIZE,BUCKET_NAME,DESTINATION_KEY,CHECKSUM,ZIP_CONTENTS
df = dd.read_csv(csv_file).drop(['FILE_SIZE','BUCKET_NAME','DESTINATION_KEY','CHECKSUM'], axis=1)
local_folder_series = df['LOCAL_FOLDER'].compute()
local_folder = local_folder_series.iloc[0]
print(local_folder)
del local_folder_series
df = df.drop(['LOCAL_FOLDER'], axis=1).compute()

print(df.head())

local_files, local_folders = get_local_file_list(local_folder)

print('Local folder:', local_folder)
print('Local files:', local_files)
print('Local folders:', local_folders)
# exclude_list = generate_exclude_list(csv_file)
# print(exclude_list)