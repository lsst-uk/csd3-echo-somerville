import matplotlib.pyplot as plt
import os, sys
import numpy as np
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
plt.suptitle('Transfer Speeds - Moving average over 100 folders')
# Smooth out the data by calculating the moving average
window_size = 100
smoothed_mib_per_sec = np.convolve(mib_per_sec, np.ones(window_size)/window_size, mode='valid')
smoothed_sec_per_file = np.convolve(sec_per_file, np.ones(window_size)/window_size, mode='valid')

# Plot the smoothed data
plt.subplot(211)
plt.plot(smoothed_mib_per_sec)
plt.xlabel('Folders')
plt.ylabel('MiB/s')

plt.subplot(212)
plt.plot(smoothed_sec_per_file)
plt.xlabel('Folders')
plt.ylabel('s/file')

# Save the figure
plt.savefig('transfer_speeds.png')