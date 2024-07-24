import pandas as pd
import os
import sys
import dask.dataframe as dd

def list_all_uploaded_leaf_dirs(df,local_folder):
    all_uploaded_dirs = []
    for root, dirs, files in os.walk(local_folder):
        print('.', end='', flush=True)
        if len(dirs) > 0:
            continue
        uploaded = []
        for filename in files:
            # if filename.endswith('.zip'): - zip files here are irrelevent. Zip files in the csv matter.
            #     continue
            if os.path.join(root,filename) in df['LOCAL_PATH'].values:
                uploaded.append(True)
            else:
                uploaded.append(False)
        if all(uploaded):
            print()
            all_uploaded_dirs.append(root)
            print(f'All files in {root} have been uploaded - adding to exclude list.')
            # print(f'Current exclude list: {all_uploaded_dirs}')
            print(f'Current exclude list length: {len(all_uploaded_dirs)}')
    return all_uploaded_dirs

csv_file = sys.argv[1]
#LOCAL_FOLDER,LOCAL_PATH,FILE_SIZE,BUCKET_NAME,DESTINATION_KEY,CHECKSUM,ZIP_CONTENTS
dtypes = {'LOCAL_FOLDER': 'str', 'LOCAL_PATH': 'str', 'FILE_SIZE': 'float', 'BUCKET_NAME': 'str', 'DESTINATION_KEY': 'str', 'CHECKSUM': 'str', 'ZIP_CONTENTS': 'str'}
df = dd.read_csv(csv_file, dtype=dtypes).drop(['FILE_SIZE','BUCKET_NAME','DESTINATION_KEY','CHECKSUM'], axis=1)
local_folder_series = df['LOCAL_FOLDER'].compute()
local_folder = local_folder_series.iloc[0]
print(local_folder)
del local_folder_series
df = df.drop(['LOCAL_FOLDER'], axis=1).compute()
basepaths = []
fnames = []
for row in df.iterrows():
    basepaths.append('/'.join(row[1]['LOCAL_PATH'].split('/')[:-1]))
    fnames.append(row[1]['LOCAL_PATH'].split('/')[-1])
    print(row[1]['ZIP_CONTENTS'], type(row[1]['ZIP_CONTENTS']))
df['BASE_PATH'] = basepaths
df['FNAME'] = fnames



print(df.head())
print(len(df))

print(sum(df['LOCAL_PATH'].str.endswith('.zip')))

uploaded_dirs = list_all_uploaded_leaf_dirs(df,local_folder)

print(len(uploaded_dirs))

with open('exclude_list.txt', 'w') as excl_f:
    excl_f.write(str(uploaded_dirs))
    excl_f.write('\n')

# print('Local folder:', local_folder)
# print('Local files:', local_files)
# print('Local folders:', local_folders)
# exclude_list = generate_exclude_list(csv_file)
# print(exclude_list)