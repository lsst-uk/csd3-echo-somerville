#!/bin/bash

# Set the default bucket name
bucket_name=LSST-IR-FUSION-TESTSTRATEGY

# Function to display usage information
usage() {
        echo "Usage: $0 [-n <nprocs>] [-d <ds>]..."
        echo "Options:"
        echo "  -n, --nprocs <nprocs>   Number of processes to use (default: 4)"
        echo "  -d, --ds <ds>           Datasets to process (list of directories)"
        echo "  -h, --help              Display this help message and exit"
        exit 1
}

# Set the default number of processes and an empty list for datasets
nprocs=4
ds_list=()

# Parse command line argumentss
while [[ $# -gt 0 ]]; do
        case "$1" in
                -n|--nprocs)
                        nprocs="$2"
                        shift 2
                        ;;
                -d|--ds)
                        ds_list+=("$2")
                        shift 2
                        ;;
                -h|--help)
                        usage
                        exit 0
                        ;;
                *)
                        echo "Unknown option: $1"
                        usage
                        exit 1
                        ;;
        esac
done

# Process each dataset
for ds in "${ds_list[@]}"; do
        local_path=$ds
        S3_folder=$ds
        
        # Backup without collation
        S3_prefix=strategy-test_dummy-data_no-collation
        python $HOME/csd3-echo-somerville/csd3-side/scripts/lsst-backup.py \
        --bucket-name $bucket_name --local-path $local_path --S3-prefix $S3_prefix \
        --S3-folder $S3_folder --nprocs $nprocs --no-collate > no-collate_${S3_prefix}_${S3_folder}_${nprocs}.log 2>&1
        rm ${S3_prefix}-${S3_folder}-lsst-backup.csv

        # Backup without compression
        S3_prefix=strategy-test_dummy-data_no-compression
        python $HOME/csd3-echo-somerville/csd3-side/scripts/lsst-backup.py \
        --bucket-name $bucket_name --local-path $local_path --S3-prefix $S3_prefix \
        --S3-folder $S3_folder --nprocs $nprocs --no-compression > no-compression_${S3_prefix}_${S3_folder}_${nprocs}.log 2>&1
        rm ${S3_prefix}-${S3_folder}-lsst-backup.csv

        # Backup with collation
        S3_prefix=strategy-test_dummy-data_collation
        python $HOME/csd3-echo-somerville/csd3-side/scripts/lsst-backup.py \
        --bucket-name $bucket_name --local-path $local_path --S3-prefix $S3_prefix \
        --S3-folder $S3_folder --nprocs $nprocs > collate_${S3_prefix}_${S3_folder}_${nprocs}.log 2>&1
        rm ${S3_prefix}-${S3_folder}-lsst-backup.csv
done

# Remove the bucket
python $HOME/csd3-echo-somerville/scripts/remove_bucket.py -y $bucket_name