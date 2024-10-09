# Bucket Names

This folder contains a JSON file that is read by (process_new_zips.py)[../dags/process_new_zips.py] to inform the Airflow DAG which buckets to scan for zip files uploaded by (lsst-backup.py)[../../csd3-side/scripts/lsst-backup.py].

Add any new buckets that have been used for backups by copying the JSON format of another bucket entry.
