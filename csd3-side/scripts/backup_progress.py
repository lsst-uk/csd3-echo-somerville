import tqdm
import sys
import string

logp = sys.argv[1]

total = 0

with open(logp, 'r') as logf:
    for line in logf:
        if 'Folders: ' in line and '; Files: ' in line:
            total = float(line.split()[3].strip().translate(str.maketrans('', '', string.punctuation)))
            break
with tqdm.tqdm(total=total) as pbar:
    progress = 0
    while True:
        with open(logp, 'r') as logf:
            for line in logf:
                if 'Folders: ' in line and '; Files: ' in line:
                    prog = float(line.split()[1].strip().translate(str.maketrans('', '', string.punctuation)))
                    if prog > progress:
                        
                        pbar.update(prog - progress)
                        progress = prog
                        break
