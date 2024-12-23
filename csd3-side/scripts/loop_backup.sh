#!/bin/bash
# Loop backup script
# This script will run the backup script until all the files are backed up
# It's kind of brute force.
config_file=$1


date
echo 'Looping backup...'

butler_prefix=$(grep 'S3_prefix:' $config_file | awk '{print $2}')
local_path=$(grep 'local_path:' $config_file | awk '{print $2}')
local_path_from_butler_prefix=${local_path#*${butler_prefix}/}
local_path_hyphens=$(echo $local_path | sed 's/\//\-/g')
collate_list_file=${butler_prefix}-${local_path_hyphens}-collate-list.csv

echo 'Using collate list file: ' $collate_list_file
echo 'Continue? (y/n)'
read continue
if [ $continue != 'y' ]; then
    echo 'Exiting...'
    exit
fi
if [ -f $collate_list_file ]; then
    test_zips=$(grep -c True $collate_list_file)
    echo 'Continuing previous looped backup...'
else
    test_zips=1 # dummy value to start the loop for a first run
    echo 'Starting new looped backup...'
fi

while [ $test_zips -gt 0 ];
do
    python ../../scripts/lsst-backup.py --config-file $config_file
    test_zips=$(grep -c True $collate_list_file)
done
date
echo 'Looped backup completed.'
