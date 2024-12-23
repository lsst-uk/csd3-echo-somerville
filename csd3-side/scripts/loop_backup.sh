#!/bin/bash
# Loop backup script
# This script will run the backup script until all the files are backed up
# It's kind of brute force.
config_file=$1
collate_list_file=$2
first_run=true
date
echo 'Looping backup...'
while [ $(grep -c True $collate_list_file) -gt 0 ] || [ $first_run = true ];
do
    echo 'while'
    python ../../scripts/lsst-backup.py --config-file $config_file
    first_run=false
done
date
echo 'Looped backup completed.'
