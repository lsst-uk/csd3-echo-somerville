#!/bin/bash
# Loop backup script
# This script will run the backup script until all the files are backed up
# It's kind of brute force.
config_file=$1
collate_list_file=$2
while [ $(grep -c True $collate_list_file) -gt 0 ]
do
    python ../../scripts/lsst-backup.py --config-file $config_file
done