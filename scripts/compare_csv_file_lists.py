import os
import argparse
import subprocess

parser = argparse.ArgumentParser(description="Compare CSV file lists in a log folder.")
parser.add_argument("--path", '-p', help="Path to the log folder", required=True)
parser.add_argument("--datestamp", '-d', help="Datestamp in the form YYYYMMDD", required=True)
args = parser.parse_args()

log_folder = args.path
datestamp = args.datestamp

def get_new_csv(log_folder, csv_files):
    log_0_csvs = []
    log_1_csvs = []
    new_csv_paths = []
    with open(f"{log_folder}/{csv_files[0]}", "r") as f:
        for line in f:
            log_0_csvs.append(line.split(',')[0].strip())
    with open(f"{log_folder}/{csv_files[1]}", "r") as f:
        for line in f:
            log_1_csvs.append(line.split(',')[0].strip())
    for csv in log_1_csvs:
        if csv not in log_0_csvs:
            new_csv_paths.append(csv)
            print(f"New CSV file: {csv}")
    return new_csv_paths

def compare_csv_file_lists(log_folder, ds):
    """
    Compare the CSV file lists in the specified log folder.

    Args:
        log_folder (str): The path to the log folder.

    Returns:
        None
    """
    csv_files = []
    for filename in os.listdir(log_folder):
        print(filename,ds)
        if filename.startswith("lsst-backup-logs-"):
            if filename.endswith(".csv"):
                if filename.__contains__(ds):
                    csv_files.append(filename)
    csv_files.sort()
    csv_files = csv_files[-2:]

    print(csv_files)

    cmp_out = subprocess.run([f'cmp {log_folder}/{csv_files[0]} {log_folder}/{csv_files[1]}'].split(), capture_output=True, text=True)

    if cmp_out.stdout != "":
        print("CSV files have changed!")
        new_csv_paths = get_new_csv(log_folder, csv_files)
        print(f"New CSV files: {new_csv_paths}")
    else:
        print("CSV files are the same.")

if __name__ == "__main__":
    compare_csv_file_lists(log_folder, datestamp)