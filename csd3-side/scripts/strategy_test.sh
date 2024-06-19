#!/bin/bash
bucket_name=LSST-IR-FUSION-TESTSTRATEGY

nprocs=4

for ds in $*
do
        local_path=$ds
        S3_folder=$ds
        
        #no collation
        S3_prefix=strategy-test_dummy-data_no-collation
        python $HOME/csd3-echo-somerville/csd3-side/scripts/lsst-backup.py \
        --bucket-name $bucket_name --local-path $local_path --S3-prefix $S3_prefix \
        --S3-folder $S3_folder --nprocs $nprocs --no-collate > no-collate_${S3_prefix}_${S3_folder}_${nprocs}.log 2>&1
        rm ${S3_prefix}-${S3_folder}-lsst-backup.csv

        #no compression
        S3_prefix=strategy-test_dummy-data_no-compression
        python $HOME/csd3-echo-somerville/csd3-side/scripts/lsst-backup.py \
        --bucket-name $bucket_name --local-path $local_path --S3-prefix $S3_prefix \
        --S3-folder $S3_folder --nprocs $nprocs --no-compression > no-compression_${S3_prefix}_${S3_folder}_${nprocs}.log 2>&1
        rm ${S3_prefix}-${S3_folder}-lsst-backup.csv

        #collation
        S3_prefix=strategy-test_dummy-data_collation
        python $HOME/csd3-echo-somerville/csd3-side/scripts/lsst-backup.py \
        --bucket-name $bucket_name --local-path $local_path --S3-prefix $S3_prefix \
        --S3-folder $S3_folder --nprocs $nprocs > collate_${S3_prefix}_${S3_folder}_${nprocs}.log 2>&1
        rm ${S3_prefix}-${S3_folder}-lsst-backup.csv
done

python $HOME/csd3-echo-somerville/scripts/remove_bucket.py -y $bucket_name