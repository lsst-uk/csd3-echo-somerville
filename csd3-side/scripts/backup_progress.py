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
with tqdm.tqdm(total=total) as pbar:
    progress = 0
    prev_len = 0
    while True:
        with open(logcsv, 'r') as logc:
            if len(logc.readlines()) - 1 > prev_len:
                logc.seek(0)
                prev_len = len(logc.readlines()) - 1
                logc.seek(0)
            else:
                continue
            prog = 0
            for line in logc:
                if 'collated_' in line:
                    prog += len(line.split('"')[-2].split(','))
                elif 'LOCAL_FOLDER' not in line:
                    prog += 1
                print(prog)
            pbar.update(prog - progress)
            progress = prog
            print(progress)
                
