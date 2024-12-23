#!/bin/bash
# Loop backup script
# This script will run the backup script until all the files are backed up
# It's kind of brute force.
# D. McKay Dec 2024

function display_help {
    echo "Usage: $0 [options]"
    echo
    echo "Options:"
    echo "  --config-file, -c FILE   Specify the configuration file."
    echo "  -y                       Automatically continue without prompting."
    echo "  -h, --help               Display this help message."
    echo
}

# Check for help option
for arg in "$@"; do
    if [[ "$arg" == "--help" || "$arg" == "-h" ]]; then
        display_help
        exit 0
    fi
done

continue=cli-n
# Parse command line arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --config-file|-c) config_file="$2"; shift ;;
        -y) continue='cli-y' ;;
        *) echo "Unknown parameter passed: $1"; display_help; exit 1 ;;
    esac
    shift
done

if [ -z "$config_file" ]; then
    echo "Error: --config-file or -c is required"
    display_help
    exit 1
fi

date
echo 'Looping backup...'

butler_prefix=$(grep 'S3_prefix:' $config_file | awk '{print $2}')
local_path=$(grep 'local_path:' $config_file | awk '{print $2}')
local_path_from_butler_prefix=${local_path#*${butler_prefix}/}
local_path_hyphens=$(echo $local_path_from_butler_prefix | sed 's/\//\-/g')
collate_list_file=${butler_prefix}-${local_path_hyphens}-collate-list.csv

echo 'Using collate list file: ' $collate_list_file
if [ $continue != 'cli-y' ]; then
    echo 'Continue? (y/n)'
    read continue
    if [ $continue != 'y' ]; then
        echo 'Exiting...'
        exit 0
    fi
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
