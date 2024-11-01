import tqdm
import sys
import string
import os

logp = sys.argv[1]
logcsv = sys.argv[2]
total = 0
with open(logp, 'r') as logf:
    for line in logf:
        if 'Folders: ' in line and ' Files: ' in line:
            total = int(line.split()[3].strip())
            break

progress = 0
prev_len = 0
prev_total_size = 0
started = False

while True:
    if prev_len > 0 and prev_total_size > 0 and not started:
        pbar_count = tqdm.tqdm(total=total, desc='Upload Progress (file count)', position=0)
        pbar_size = tqdm.tqdm(total=0, desc='Upload Progress (file size / MiB)', position=1)
        started = True
    with open(logcsv, 'r') as logc:
        if len(logc.readlines()) - 1 > prev_len:
            logc.seek(0)
            prev_len = len(logc.readlines()) - 1
            logc.seek(0)
        else:
            continue
        prog = 0
        total_size = 0
        for line in logc:
            if 'collated_' in line:
                prog += len(line.split('"')[-2].split(','))
            elif not 'LOCAL_FOLDER' in line:
                prog += 1
            if not 'LOCAL_FOLDER' in line:
                total_size += int(line.split(',')[2])
        if total_size > prev_total_size:
            prev_total_size = total_size
        if started:
            pbar_count.update(prog - progress)
            pbar_size.update((total_size - prev_total_size) / (1024 * 1024))
            progress = prog
            prev_total_size = total_size