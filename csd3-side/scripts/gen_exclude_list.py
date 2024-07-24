import pandas as pd
import os
import sys
import dask.dataframe as dd

def list_all_uploaded_leaf_dirs(df,local_folder):
    for root, dirs, files in os.walk(local_folder):
        if len(dirs) > 0:
            continue
        all_uploaded_dirs = []
        uploaded = []
        for f in files:
            if f.endswith('.zip'):
                continue
            if os.path.join([root,f]) in df['LOCAL_PATH'].values:
                uploaded.append(True)
            else:
                uploaded.append(False)
        print(uploaded)
        if all(uploaded):
            all_uploaded_dirs.append(root)
        print(len(all_uploaded_dirs))
    return all_uploaded_dirs

csv_file = sys.argv[1]
#LOCAL_FOLDER,LOCAL_PATH,FILE_SIZE,BUCKET_NAME,DESTINATION_KEY,CHECKSUM,ZIP_CONTENTS
df = dd.read_csv(csv_file).drop(['FILE_SIZE','BUCKET_NAME','DESTINATION_KEY','CHECKSUM'], axis=1)
local_folder_series = df['LOCAL_FOLDER'].compute()
local_folder = local_folder_series.iloc[0]
print(local_folder)
del local_folder_series
df = df.drop(['LOCAL_FOLDER'], axis=1).compute()

print(df.head())
print(len(df))

uploaded_dirs = list_all_uploaded_leaf_dirs(df,local_folder)

print(len(uploaded_dirs))

# print('Local folder:', local_folder)
# print('Local files:', local_files)
# print('Local folders:', local_folders)
# exclude_list = generate_exclude_list(csv_file)
# print(exclude_list)