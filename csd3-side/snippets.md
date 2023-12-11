# Code ideas
These code snippets are untested and may not work have completely unexpected effects. They are just gathered ideas.

## Find and upload files using all available CPU cores
```python
import boto3
import os
from multiprocessing import Pool

def copy_files_to_bucket(source_file, destination_key, filename):
    # Upload the file to the bucket
    s3.Object(bucket_name, destination_key).upload_file(source_file)

    # Write the file name to the list
    with open(filename, 'a') as file:
        file_name = os.path.basename(source_file)
        file.write('%s\n' % file_name)

def process_files(source_dir, bucket_name, destination_dir, filename):
    files = os.listdir(source_dir)
    with Pool() as pool:
        pool.map(copy_files_to_bucket, 
            [os.path.join(source_dir, file) for file in files],
            [os.path.join(destination_dir, file) for file in files],
            [filename])

# Set the source directory, bucket name, and destination directory
source_dir = "/path/to/source/directory"
bucket_name = "your-ceph-s3-bucket"
destination_dir = "/path/to/destination/directory"
filename = "files.txt"

# Process the files in parallel
process_files(source_dir, bucket_name, destination_dir, filename)
```

## Find and upload files using all available CPU cores - uses os.walk
```python
import boto3
import os
from multiprocessing import Pool

def copy_files_to_bucket(root_dir, bucket_name, destination_dir, filename):
    for dirpath, dirnames, filenames in os.walk(root_dir):
        destination_key = os.path.join(destination_dir, os.path.relpath(dirpath, root_dir))

        if not s3.Object(bucket_name, destination_key).exists():
            s3.Object(bucket_name, destination_key).mkdir()

        for filename in filenames:
            source_file = os.path.join(dirpath, filename)
            destination_key = os.path.join(destination_dir, os.path.relpath(source_file, root_dir))

            # Upload the file to the bucket
            s3.Object(bucket_name, destination_key).upload_file(source_file)

            # Write the file name to the list
            file_name = os.path.basename(source_file)
            with open(filename, 'a') as file:
                file.write('%s\n' % file_name)

def process_files(source_dir, bucket_name, destination_dir, filename):
    with Pool() as pool:
        for _, _, filenames in os.walk(source_dir):
            for filename in filenames:
                pool.apply_async(copy_files_to_bucket, args=(source_dir, bucket_name, destination_dir, filename))

# Set the source directory, bucket name, and destination directory
source_dir = "/path/to/source/directory"
bucket_name = "your-ceph-s3-bucket"
destination_dir = "/path/to/destination/directory"
filename = "files.txt"

# Process the files in parallel
process_files(source_dir, bucket_name, destination_dir, filename)
```

## Upload files and checksum otf
```python
import os
import hashlib
from multiprocessing import Pool

def upload_and_checksum_files(directory):
    for root, dirnames, filenames in os.walk(directory):
        for filename in filenames:
            filepath = os.path.join(root, filename)
            file_extension = filepath.split('.')[-1]

            # Upload the file to S3
            s3_conn = S3Hook()
            s3_conn.load_file(filepath,
                              bucket_name='your-ceph-s3-bucket',
                              key=filepath.replace('/', '-'))

            # Calculate the checksum of the file
            file_data = open(filepath, 'rb').read()
            md5_checksum = hashlib.md5(file_data).hexdigest()

            # Generate the corresponding .md5 file
            md5_file_key = filepath + '.' + file_extension + '.md5'
            md5_data = md5_checksum.encode('utf-8')
            s3_conn.put_object(bucket_name='your-ceph-s3-bucket',
                            key=md5_file_key,
                            Body=md5_data)

# Set the directory to upload files from
directory = "/path/to/directory"

# Upload and checksum files in parallel
pool = Pool()
pool.map(upload_and_checksum_files, [directory])
```
