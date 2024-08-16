import os
import argparse
import subprocess

def split_csv_file_list(csv_file_list):
    """
    Split the CSV file list into two lists; one for backup logs and one for verification logs.

    Args:
        csv_file_list (list): A list of CSV files.

    Returns:
        tuple: backup_logs, verification_logs
    """
    backup_logs = []
    verification_logs = []
    
    for csv_file in csv_file_list:
        if csv_file.__endswith__(log_suffix) or csv_file.__endswith__(previous_log_suffix):
            backup_logs.append(csv_file)
        elif csv_file.__endswith__(verification_suffix):
            verification_logs.append(csv_file)
    return backup_logs, verification_logs

def compare_csv_file_lists(backup_logs, verification_logs):
    """
    Compare lists of backup logs and verification logs to determine if any new CSV files have been uploaded.

    Args:
        backup_logs (list): A list of backup logs.
        verification_logs (list): A list of verification logs.

    Returns:
        List: A list of new backup log CSV files yet to be verified.
    """
    
    to_verify = []
    for backup_log in backup_logs:
        if backup_log.endswith(previous_log_suffix):
            verification_equiv = backup_log.replace(f'-{previous_log_suffix}',f'-{verification_suffix}')
        else:
            verification_equiv = backup_log.replace(f'-{log_suffix}',f'-{verification_suffix}')
        if verification_equiv not in verification_logs:
            to_verify.append(backup_log)

    return to_verify

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compare CSV file lists in a log folder.")
    parser.add_argument("--from-file", "-f", type=str, help="Absolute path to a file containing a list of CSV files, one per line.", default=None)
    parser.add_argument("--from-arg", "-a", type=str, help="A comma-separated list of CSV files.", default=None)
    parser.add_argument("--to-file", "-t", type=str, help="Absolute path to a file to write the list of new backup logs to verify.", default=None)
    parser.add_argument("--debug", "-d", action="store_true", help="Print debug information.", default=False)

    args = parser.parse_args()

    log_suffix = 'lsst-backup.csv'
    previous_log_suffix = 'files.csv'
    verification_suffix = 'lsst-backup-verification.csv'

    csv_file_list = []

    if args.from_file:
        with open(args.from_file, "r") as f:
            for line in f:
                csv_file_list.append(line.strip())
    elif args.from_arg:
        csv_file_list = args.from_arg.split(",")
    else:
        print("No CSV file list provided. Exiting.")
        exit(1)

    outfile = args.to_file

    backup_logs, verification_logs = split_csv_file_list(csv_file_list)

    to_verify = compare_csv_file_lists(backup_logs, verification_logs)

    if args.debug:
        print(f"CSV file list: {csv_file_list}")
        
        print(f"Backup logs: {backup_logs}, {len(backup_logs)}")
        print(f"Verification logs: {verification_logs}, {len(verification_logs)}")
        
        print(f"New backup logs to verify: {to_verify}, {len(to_verify)}")

    if outfile:
        with open(outfile, "w") as f:
            for log in to_verify:
                f.write(f"{log}\n")
        print(f"New backup logs to verify written to {outfile}.")
    else:
        for log in to_verify:
            print(log)