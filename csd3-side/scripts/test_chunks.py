import os
import sys
import zipfile
from multiprocessing import Pool

def process_files(directory, nprocs):
    # Get a list of all subfolders
    subfolders = [f.path for f in os.scandir(directory) if f.is_dir()]
    print(subfolders)
    # Divide the list of subfolders into chunks
    chunks = [subfolders[i:i + nprocs] for i in range(0, len(subfolders), nprocs)]
    
# if num_folders / nprocs < 4:
# ...     underpopulate = True
#     if underpopulate:
# ...     b = [[x for y in a for x in y][i:i + 4] for i in range(0, len([x for y in a for x in y]), 4)]

    print(chunks)

    # Use a multiprocessing pool to process each chunk
    with Pool(nprocs) as p:
        p.map(process_chunk, chunks)

def process_chunk(chunk):
    # Create a new zip file for this chunk
    # with zipfile.ZipFile('archive.zip', 'w') as zipf:
    #     # Add each file in the chunk to the zip file
    #     for folder in chunk:
    #         for root, dirs, files in os.walk(folder):
    #             for file in files:
    #                 zipf.write(os.path.join(root, file))
    print(chunk)

directory = sys.argv[1]
nprocs = int(sys.argv[2])

process_files(directory, nprocs)