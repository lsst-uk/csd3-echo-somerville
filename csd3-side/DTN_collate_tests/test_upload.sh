#!/bin/bash
#clean bucket
clean_bucket () {
	python ../../scripts/remove_bucket.py -y LSST-IR-FUSION-TESTCOLLATE >/dev/null 2>&1
}
cd /home/ir-mcka1/csd3-echo-somerville/csd3-side/test_collate_DTN
NPROCS=56
clean_bucket
for i in 34 64 128 256 512 1024
do
	python ../scripts/lsst-backup.py LSST-IR-FUSION-TESTCOLLATE /rds/project/rds-rPTGgs6He74/davem/dummy_data/dummy_butler/data/d_${i}_1fpf/ dummy_data d_${i}_1fpf --nprocs ${NPROCS} --no-collate --no-compression > no-collate_no-compression_test_${i}_1fpf_${NPROCS}procs.log 2>&1
	rm dummy_data-d_${i}_1fpf-lsst-backup.csv
done
clean_bucket
for i in 34 64 128 256 512 1024
do
        python ../scripts/lsst-backup.py LSST-IR-FUSION-TESTCOLLATE /rds/project/rds-rPTGgs6He74/davem/dummy_data/dummy_butler/data/d_${i}_1fpf/ dummy_data d_${i}_1fpf --nprocs ${NPROCS} --no-compression > collate_no-compression_test_${i}_1fpf_${NPROCS}procs.log 2>&1
	rm dummy_data-d_${i}_1fpf-lsst-backup.csv
done
clean_bucket
for i in 34 64 128 256 512 1024
do
        python ../scripts/lsst-backup.py LSST-IR-FUSION-TESTCOLLATE /rds/project/rds-rPTGgs6He74/davem/dummy_data/dummy_butler/data/d_${i}_1fpf/ dummy_data d_${i}_1fpf --nprocs ${NPROCS}  > collate_compression_test_${i}_1fpf_${NPROCS}procs.log 2>&1
	rm dummy_data-d_${i}_1fpf-lsst-backup.csv
done
clean_bucket
