import os
import argparse

parser = argparse.ArgumentParser(description="Compare CSV file lists in a log folder.")
parser.add_argument("--path", '-p', help="Path to the log folder", required=True)
args = parser.parse_args()

log_folder = args.path

def compare_csv_file_lists(log_folder):
    """
    Compare the CSV file lists in the specified log folder.

    Args:
        log_folder (str): The path to the log folder.

    Returns:
        None
    """
    csv_files = []
    for filename in os.listdir(log_folder):
        print(filename)
        ds = '{{ ds_nodash }}'
        if filename.startswith("lsst-backup-logs-") and filename.endswith(".csv") and filename.__contains__(ds):
            csv_files.append(filename.replace("lsst-backup-logs-" + ds + "T", "").replace(".csv", ""))
    csv_files.sort()
    csv_files = csv_files[-2:]

    print(csv_files)

if __name__ == "__main__":
    compare_csv_file_lists(log_folder)