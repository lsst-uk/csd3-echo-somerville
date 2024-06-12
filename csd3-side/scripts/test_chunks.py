import os
import sys
import zipfile
from multiprocessing import Pool

def process_files(directory, nprocs):
    # Get a list of all subfolders
    subfolders = [f.path for f in os.scandir(directory) if f.is_dir()]
    # print(subfolders)
    # Divide the list of subfolders into chunks
    chunks = [subfolders[i:i + nprocs] for i in range(0, len(subfolders), nprocs)]
    
# if num_folders / nprocs < 4:
# ...     underpopulate = True
#     if underpopulate:
# ...     b = [[x for y in a for x in y][i:i + 4] for i in range(0, len([x for y in a for x in y]), 4)]

    # print(chunks)

    # Use a multiprocessing pool to process each chunk
    p = Pool(nprocs)
    results = p.map(process_chunk, zip(chunks, list(range(len(chunks)))))
    p.close()
    p.join()
    return results

def process_chunk(chunk):
    # Create a new zip file for this chunk
    chunk_folders, chunk_id = chunk
    zipfile_name = f'zip_{chunk_id}.zip'
    print(f'chunk {chunk}')
    print(f'chunk_folders {chunk_folders}')
    print(f'chunk_id {chunk_id}')
    print(f'zipfile_name {zipfile_name}')
    with zipfile.ZipFile(zipfile_name, 'w') as zipf:
        # Add each file in the chunk to the zip file
        for folder in chunk_folders:
            for root, dirs, files in os.walk(folder):
                for file in files:
                    zipf.write(os.path.join(root, file))
    print(f'{zipfile_name} done')
    return str(chunk_folders), zipfile_name

directory = sys.argv[1]
nprocs = int(sys.argv[2])

results = process_files(directory, nprocs)

for result in results:
    print(result)