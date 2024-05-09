import matplotlib.pyplot as plt
import os, sys
import numpy as np
if len(sys.argv) != 2:
    print(f'Usage: {sys.argv[0]} <logfile>')
    sys.exit(1)
logfile = sys.argv[1]
title_name = logfile.split("/")[-1].split(".")[0]
# Define the regular expression pattern to match the desired lines
pattern = 'uploaded in'  # Modified pattern to match both lines

# Lists to store the extracted data
mbps = []
sec_per_file = []

with open(logfile, 'r') as lf:
    for line in lf.readlines():
        if pattern in line:
            #Possible lines:
            #3.01 MiB uploaded in 0.74 seconds, 4.09 MiB/s
            #2 files (avg 3.17 MiB/file) uploaded in 0.43 seconds, 0.22 s/file
            if 'MiB/s' in line:
                mbps.append(float(line.split()[6])*8)
            elif 's/file' in line:
                sec_per_file.append(float(line.split()[9]))


# Create the plots
plt.figure(1)

# Smooth out the data by calculating the moving average
window_size = 10
smoothed_mib_per_sec = np.convolve(mbps, np.ones(window_size)/window_size, mode='valid')
smoothed_sec_per_file = np.convolve(sec_per_file, np.ones(window_size)/window_size, mode='valid')
plt.suptitle(f'Transfer Speeds - Moving average over {window_size} folders.\n{title_name}')
# Plot the smoothed data
plt.subplot(211)
plt.plot(smoothed_mib_per_sec)
plt.xlabel('Folders')
plt.ylabel('Mbit/s')

plt.subplot(212)
plt.plot(smoothed_sec_per_file)
plt.xlabel('Folders')
plt.ylabel('s/file')
plt.tight_layout()
# Save the figure
plt.savefig(f'transfer_speeds_{title_name}.png')