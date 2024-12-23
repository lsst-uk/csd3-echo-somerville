#!/bin/bash
# Loop backup script
# This script will run the backup script until all the files are backed up
# It's kind of brute force.
config_file=$1
collate_list_file=$2

date
echo 'Looping backup...'
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
