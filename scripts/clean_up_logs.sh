#!/bin/bash

# This script is used to clean up log files in a specified folder.
# It checks if the log folder exists, counts the number of log files with a specific naming pattern,
# and deletes log files that are older than 1 day if doing so will leave at least two log files.

log_folder=$1

# Check if the log folder exists
if [ ! -d "$log_folder" ]; then
    echo "Log folder does not exist."
    exit 1

# Count files with naming pattern "lsst-backup-logs-*.csv"
else
    num_files=$(ls -1 $log_folder/lsst-backup-logs-*.csv 2>/dev/null | wc -l)
    if [ $num_files -gt 2 ]; then
        num_old=`find $log_folder -name "lsst-backup-logs-*.csv" -type f -mtime +1 | wc -l`
        num_left=`expr $num_files - $num_old`
        if [[ $num_left -gt 2 ]];
        then
            echo "Cleaning up $num_files log files..."
            find $log_folder -name "lsst-backup-logs-*.csv" -type f -mtime +1 -exec rm -v {} \;
        fi
        echo "Log files cleaned up."
    else
        echo "No log files to clean up."
    fi
fi
