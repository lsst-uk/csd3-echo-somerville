import pandas as pd
import os
import sys
import dask.dataframe as dd

def list_all_uploaded_leaf_dirs(df):
    all_uploaded_dirs = []
    df.groupby('LOCAL_FOLDER').count()
    for root, dirs, files in os.walk(local_folder):
        print('.', end='', flush=True)
        if len(dirs) > 0:
            continue
        uploaded = []
        for filename in files:
            # if filename.endswith('.zip'): - zip files here are irrelevent. Zip files in the csv matter.
            #     continue
            if os.path.join(root,filename) in df['LOCAL_FILENAME'].values:
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

def verify_zipped_dirs(zipped_dirs_df):
    unique_zipped_dirs = zipped_dirs_df['BASE_PATH'].unique()
    print(unique_zipped_dirs)
    verified_zipped = []
    for u_z_d in unique_zipped_dirs:
        these_zipped_dirs = zipped_dirs_df.loc[zipped_dirs_df['BASE_PATH'] == u_z_d]
        print(these_zipped_dirs)
        these_verified_zipped = []
        for t_z_d in these_zipped_dirs.iterrows():
            these_verified_zipped.extend(t_z_d[1]['ZIP_CONTENTS'].split(','))
        files_in_basepath = [ f for _, _, files in os.walk(u_z_d) for f in files ]
        verified = []
        for f_in_b in files_in_basepath:
            if f_in_b in these_verified_zipped:
                verified.append(True)
            else:
                verified.append(False)
        if all(verified):
            print(f'All files in {u_z_d} have been uploaded - adding to exclude list.')
            verified_zipped.append(u_z_d)
            print(f'Current exclude list length: {len(verified_zipped)}')
    print(len(verified_zipped))

    # return verified_zipped

csv_file = sys.argv[1]
#LOCAL_FOLDER,LOCAL_PATH,FILE_SIZE,BUCKET_NAME,DESTINATION_KEY,CHECKSUM,ZIP_CONTENTS
dtypes = {'LOCAL_FOLDER': 'str', 'LOCAL_PATH': 'str', 'FILE_SIZE': 'float', 'BUCKET_NAME': 'str', 'DESTINATION_KEY': 'str', 'CHECKSUM': 'str', 'ZIP_CONTENTS': 'str'}
df = dd.read_csv(csv_file, dtype=dtypes).drop(['FILE_SIZE','BUCKET_NAME','DESTINATION_KEY','CHECKSUM'], axis=1).compute()
local_fns = []
for row in df.iterrows():
    print(row[1]['LOCAL_PATH'])
    print(row[1]['LOCAL_FOLDER'])
    
    if row[1]['LOCAL_PATH'].startswith(row[1]['LOCAL_FOLDER']):
        local_fns.append(row[1]['LOCAL_PATH'][len(row[1]['LOCAL_FOLDER'])+1:])
    else:
        local_fns.append(row[1]['LOCAL_PATH'])
df['LOCAL_FILENAME'] = local_fns
df = df.drop(['LOCAL_PATH'], axis=1)
df.loc[df['ZIP_CONTENTS'].isna(), 'ZIP_CONTENTS'] = ''



print(df.head())
print(df.tail())
df = df.groupby('LOCAL_FOLDER').agg(','.join).reset_index().dropna()
print(df['LOCAL_FOLDER'].head())
df.to_csv('test.csv', index=False)

# print(len(df))

### USE LOCAL_FILENAME NOT LOCAL_PATH

# print(sum(df['LOCAL_FILENAME'].str.endswith('.zip')))

# uploaded_dirs = list_all_uploaded_leaf_dirs(df)

# print(len(uploaded_dirs))

# zipped_dirs_df = df[df['LOCAL_PATH'].str.endswith('.zip')]
# print(len(zipped_dirs_df))
# verify_zipped = verify_zipped_dirs(zipped_dirs_df)
# uploaded_dirs.extend(verify_zipped)

# print(len(uploaded_dirs))
j=0
for folder_row in df.iterrows():
    logged_files = folder_row[1]['LOCAL_FILENAME'].split(',')
    logged_zipped_files = folder_row[1]['ZIP_CONTENTS'].split(',')
    # logged_zipped_files = [szf.split('/')[-1] for szf in logged_zipped_files]
    local_folder = folder_row[1]['LOCAL_FOLDER']
    collated = False
    if logged_files[0].endswith('.zip') and logged_zipped_files[0] != '':
        collated = True
    elif logged_files[0].endswith('.zip') and logged_zipped_files[0] == '':
        raise ValueError('Zip file with no contents')
    elif logged_files[0].endswith('.zip') == False and logged_zipped_files[0] != '':
        raise ValueError('Non-zip file with zip contents')
    elif logged_files[0].endswith('.zip') == False and logged_zipped_files[0] == '':
        collated = False

    print(f"local_folder: {local_folder}")
    print(f"logged_files: {logged_files}")
    print(f"logged_zipped_files: {logged_zipped_files}")
    
    if collated:
        extend_path = ''
        for i, lzf in enumerate(logged_zipped_files):
            if '/' in lzf:
                lzf_list = lzf.split('/')
                logged_zipped_files[i] = lzf_list[-1]
                extend_path = '/' + '/'.join(lzf_list[:-1])
        local_folder += extend_path
        print(f'Folder: {local_folder}')
        print('Collated')
        print('Verifying files...')
        files = [f for _,_,files in os.walk(local_folder) for f in files]
        print(files)
        print(logged_zipped_files)
        if all([f in files for f in logged_zipped_files]) and len(files) == len(logged_zipped_files):
            print('Verified')
        else:
            print('Unverified')
        
    else:
        print(f'Folder: {local_folder}')
        print('Not collated')
        print('Verifying files...')
        files = [f for _,_,files in os.walk(local_folder) for f in files]
        print(files)
        print(logged_files)
        if all([f in files for f in logged_files]) and len(files) == len(logged_files):
            print('Verified')
        else:
            print('Unverified')


    j+=1
    if j>5:
        break

# with open('exclude_list.txt', 'w') as excl_f:
#     excl_f.write(str(uploaded_dirs))
#     excl_f.write('\n')

# print('Local folder:', local_folder)
# print('Local files:', local_files)
# print('Local folders:', local_folders)
# exclude_list = generate_exclude_list(csv_file)
# print(exclude_list)