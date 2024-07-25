import pandas as pd
import os
import sys
import dask.dataframe as dd

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


j=0
exclude_list = []
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
        # extend_path = ''
        for i, lzf in enumerate(logged_zipped_files):
            if '/' in lzf:
                lzf_list = lzf.split('/')
                logged_zipped_files[i] = lzf_list[-1]
        #         extend_path = '/' + '/'.join(lzf_list[:-1])
        # local_folder += extend_path
        print(f'Folder: {local_folder}')
        print('Collated')
        print('Verifying files...')
        for root, dirs, files in os.walk(local_folder):
            print(root)
            print(dirs)
            print(files)
        files = [f for _,_,files in os.walk(local_folder) for f in files]
        print(files)
        print(logged_zipped_files)
        if all([f in files for f in logged_zipped_files]) and len(files) == len(logged_zipped_files):
            print('Verified')
            exclude_list.append(local_folder)
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
            exclude_list.append(local_folder)
        else:
            print('Unverified')


    j+=1
    if j>5:
        break
print(exclude_list)
with open('exclude_list.txt', 'w') as excl_f:
    excl_f.write(str(exclude_list))
    excl_f.write('\n')
