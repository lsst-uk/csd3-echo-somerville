import matplotlib.pyplot as plt
import os, sys
if len(sys.argv) != 2:
    print(f'Usage: {sys.argv[0]} <logfile>')
    sys.exit(1)
logfile = sys.argv[1]
# Define the regular expression pattern to match the desired lines
pattern = 'uploaded in'  # Modified pattern to match both lines

# Lists to store the extracted data
mib_per_sec = []
sec_per_file = []

with open(logfile, 'r') as lf:
    for line in lf.readlines():
        if pattern in line:
            #Possible lines:
            #3.01 MiB uploaded in 0.74 seconds, 4.09 MiB/s
            #2 files (avg 3.17 MiB/file) uploaded in 0.43 seconds, 0.22 s/file
            if 'MiB/s' in line:
                mib_per_sec.append(float(line.split()[6]))
            elif 's/file' in line:
                sec_per_file.append(float(line.split()[9]))

# Create the plots
plt.figure(1)
plt.suptitle('Transfer Speeds')
plt.subplot(211)
plt.plot(mib_per_sec)
plt.xlabel('Folder')
plt.ylabel('MiB/s')

plt.subplot(212)
plt.plot(sec_per_file)
plt.xlabel('Folder')
plt.ylabel('s/file')

# Show the plots
plt.savefig('transfer_speeds.png')