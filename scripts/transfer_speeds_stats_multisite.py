#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import matplotlib.pyplot as plt
import glob
import os
import numpy as np
from dask import dataframe as dd
from distributed import Client


platforms = ['cirrus', 'somerville', 'csd3dtn', 'stfc']
compressions = ['Comp', 'NoComp']
platform = platforms[2]
compression = compressions[0]
workerss = [8, 16, 32, 48]
workers = workerss[0]
threadss = [1, 2]
threads = threadss[1]
base_paths = []
for p in platforms:
    for c in compressions:
        for w in workerss:
            for t in threadss:
                base_path = f'{os.environ["HOME"]}/testing_by_platform/{p}/{c}_{w}-{t}'
                if os.path.exists(base_path) and os.path.isdir(base_path):
                    base_paths.append(base_path)

paths = []
for bp in base_paths:
    p = glob.glob(bp+'/*-lsst-backup.csv')[0]
    print(p)
    paths.append(p)

path = paths[0]
this_base_path = os.path.dirname(path)
path

df = pd.read_csv(path).drop(columns=['LOCAL_FOLDER', 'DESTINATION_KEY', 'CHECKSUM'])

df.sort_values(by='UPLOAD_START')
print(df)

df['FILES_PER_ZIP'] = df['FILES_PER_ZIP'].fillna(0).astype(int)

df['FILES_PER_ZIP'] = df['FILES_PER_ZIP'].astype('Int64')
df['UPLOAD_END'] = pd.to_datetime(df['UPLOAD_END'], format='%Y-%m-%d %H:%M:%S.%f', errors='coerce')
df['UPLOAD_START'] = pd.to_datetime(df['UPLOAD_START'], format='%Y-%m-%d %H:%M:%S.%f', errors='coerce')
df['FILE_SIZE'] = df['FILE_SIZE'].astype('Int64')
df = df.dropna(subset=['UPLOAD_END', 'UPLOAD_START', 'FILE_SIZE'])
print(df)

df['OVERLAP'] = df['UPLOAD_END'].shift(1) > df['UPLOAD_START']


df = df.drop(columns=['ZIP_CONTENTS'])

print(f'Instances of overlap: {df["OVERLAP"].sum()} vs instanses of no overlap: {(~df["OVERLAP"]).sum()}')

df['TYPE'] = df['LOCAL_PATH'].apply(lambda x: 'zip' if x.endswith('.zip') else 'file')
df.drop(columns=['LOCAL_PATH'], inplace=True)


df['COMPRESSION_USED'] = df.apply(lambda row: True if compression == 'Comp' and row['TYPE'] == 'zip' else False, axis=1)
print(df)

print(len(df[df['TYPE'] == 'zip']), len(df[df['TYPE'] == 'file']))

print(f'Total file size: {df['FILE_SIZE'].sum() / 1024**3} GiB')

zip_upload_time = df[df['TYPE'] == 'zip']['UPLOAD_TIME'].sum()
print(f'Zip upload time: {zip_upload_time}')

print(f"Total upload time for zip files: {df[df['TYPE'] == 'zip']['UPLOAD_TIME'].sum():.0f} seconds")
print(f"Average upload time for zip files: {df[df['TYPE'] == 'zip']['UPLOAD_TIME'].mean():.2f} seconds")
print(f"Avergage number of files per zip: {df[df['TYPE'] == 'zip']['FILES_PER_ZIP'].mean():.2f}")
print(f"Total files in zips: {df[df['TYPE'] == 'zip']['FILES_PER_ZIP'].sum():.0f}")
print(f"Average upload time per file in zips: {zip_upload_time / df[df['TYPE'] == 'zip']['FILES_PER_ZIP'].sum():.2f} seconds")
print(f"Total upload time for file uploads: {df[df['TYPE'] == 'file']['UPLOAD_TIME'].sum():.0f} seconds")
print(f"Average upload time for file uploads: {df[df['TYPE'] == 'file']['UPLOAD_TIME'].mean():.2f} seconds")
print(f"Total upload time: {df['UPLOAD_TIME'].sum():.0f} seconds")
print(f"Average upload time: {df['UPLOAD_TIME'].mean():.2f} seconds")


zip_total_size = df[df['TYPE'] == 'zip']['FILE_SIZE'].sum()
print(f'Total zip size: {zip_total_size / (1024 * 1024 * 1024)} GiB')

file_total_size = df[df['TYPE'] == 'file']['FILE_SIZE'].sum()
print(f'Total individual file size: {file_total_size / (1024 * 1024 * 1024)} GiB')

print(f"Zip file size stats (MB):")
print(df[df['TYPE'] == 'zip']['FILE_SIZE'].describe() / (1024 * 1024))

print(f"File size stats (MB):")
print(df[df['TYPE'] == 'file']['FILE_SIZE'].describe() / (1024 * 1024))

df['TRANSFER_RATE'] = (df['FILE_SIZE'] * 8) / 1024**3 / df['UPLOAD_TIME']
print(f"Transfer rate stats (Gb/s):")
print(df['TRANSFER_RATE'].describe())

plt.figure(figsize=(10, 6))
plt.hist(df[df['TYPE'] == 'zip'][df['COMPRESSION_USED']]['FILE_SIZE']/1024**2, bins=50, color='blue', alpha=0.7, edgecolor='black')
plt.title('Zip File Size Distribution (with compression)')
plt.xlabel('File Size (MiB)')
plt.ylabel('Frequency')
# plt.xlim(0, 10)
# plt.ylim(0, 200)
# plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.show()

plt.figure(figsize=(10, 6))
plt.hist(df[df['TYPE'] == 'zip'][~df['COMPRESSION_USED']]['FILE_SIZE']/1024**2, bins=50, color='orange', alpha=0.7, edgecolor='black')
plt.title('Zip File Size Distribution (without compression)')
plt.xlabel('File Size (MiB)')
plt.ylabel('Frequency')
# plt.xlim(0, 10)
# plt.ylim(0, 200)
# plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.show()

plt.figure(figsize=(10, 6))

# Prepare the data for the stacked histogram
comp_sizes = df[(df['TYPE'] == 'zip') & (df['COMPRESSION_USED'])]['FILE_SIZE'] / 1024**2
nocomp_sizes = df[(df['TYPE'] == 'zip') & (~df['COMPRESSION_USED'])]['FILE_SIZE'] / 1024**2

# Plot the stacked histogram
plt.hist([comp_sizes, nocomp_sizes], 
         bins=50, 
         color=['blue', 'orange'], 
         alpha=0.7, 
         edgecolor='black',
         stacked=True, 
         label=['With Compression', 'Without Compression'])

plt.title('Zip File Size Distribution (with and without compression)')
plt.xlabel('File Size (MiB)')
plt.ylabel('Frequency')
plt.legend()
# plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.show()

plt.figure(figsize=(10, 6))
plt.hist(df[df['TYPE'] == 'zip'][df['COMPRESSION_USED']]['TRANSFER_RATE'], bins=50, color='blue', alpha=0.7, edgecolor='black')
plt.title('Zip File Transfer Rate Distribution')
plt.xlabel('Transfer Rate (Gbps)')
plt.ylabel('Frequency')
# plt.xlim(0, 10)
# plt.ylim(0, 200)
# plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.show()

plt.figure(figsize=(10, 6))
plt.hist(df[df['TYPE'] == 'zip'][~df['COMPRESSION_USED']]['TRANSFER_RATE'], bins=50, color='orange', alpha=0.7, edgecolor='black')
plt.title('Zip File Transfer Rate Distribution')
plt.xlabel('Transfer Rate (Gbps)')
plt.ylabel('Frequency')
# plt.xlim(0, 10)
# plt.ylim(0, 200)
# plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.show()

plt.figure(figsize=(10, 6))

# Prepare the data for the stacked histogram
comp_rates = df[(df['TYPE'] == 'zip') & (df['COMPRESSION_USED'])]['TRANSFER_RATE']
nocomp_rates = df[(df['TYPE'] == 'zip') & (~df['COMPRESSION_USED'])]['TRANSFER_RATE']

# Plot the stacked histogram
plt.hist([comp_rates, nocomp_rates], 
         bins=50, 
         color=['blue', 'orange'], 
         alpha=0.7, 
         edgecolor='black',
         stacked=True, 
         label=['With Compression', 'Without Compression'])

plt.title('Zip File Transfer Rate Distribution (Stacked)')
plt.xlabel('Transfer Rate (Gbps)')
plt.ylabel('Frequency')
plt.legend()
# plt.xlim(0, 10)
# plt.ylim(0, 200)
# plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.show()

plt.figure(figsize=(10, 6))
plt.scatter(df[df['TYPE'] == 'zip']['FILE_SIZE'] / 1024**2, df[df['TYPE'] == 'zip']['TRANSFER_RATE'], alpha=0.5)
plt.title('Zip File Size vs Transfer Rate')
# plt.xlim(0,3000)
# plt.ylim(0, 15)
plt.xlabel('File Size (MiB)')
plt.ylabel('Transfer Rate (Gbps)')
plt.show()

# Calculate the rolling average of the transfer rate over a window of 100
rolling_avg = df[df['TYPE'] == 'zip']['TRANSFER_RATE'].mean()

# Plotting the rolling average transfer rate over the index
plt.figure(figsize=(10, 6))
plt.plot(rolling_avg)
plt.title('Rolling Average Transfer Rate Over Index (Window=1000)')
# plt.xlim(0,3000)
# plt.ylim(0, 15)
plt.xlabel('Zip file transfers')
plt.ylabel('Transfer Rate (Gbps)')
plt.show()

plt.figure(figsize=(10, 6))
plt.hist(df[df['TYPE'] == 'zip']['FILES_PER_ZIP'], bins=50, color='blue', alpha=0.7, edgecolor='black')
plt.title('Number of files per zip file Distribution')
plt.xlabel('Files per Zip')
plt.ylabel('Frequency')
# plt.xlim(0, 10)
# plt.ylim(0, 200)
# plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.show()

plt.figure(figsize=(10, 6))
plt.hist(df[df['TYPE'] == 'file']['FILE_SIZE']/1024**2, bins=50, color='blue', alpha=0.7, edgecolor='black')
plt.title('Individual File Size Distribution')
plt.xlabel('File Size (MiB)')
plt.ylabel('Frequency')
# plt.xlim(0, 10)
# plt.ylim(0, 200)
# plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.show()

plt.figure(figsize=(10, 6))
plt.hist(df[df['TYPE'] == 'file']['TRANSFER_RATE'], bins=50, color='blue', alpha=0.7, edgecolor='black')
plt.title('Individual File Transfer Rate Distribution')
plt.xlabel('Transfer Rate (Gbps)')
plt.ylabel('Frequency')
# plt.xlim(0, 10)
# plt.ylim(0, 200)
# plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.show()

plt.figure(figsize=(10, 6))
plt.scatter(df[df['TYPE'] == 'file']['FILE_SIZE'] / 1024**2, df[df['TYPE'] == 'file']['TRANSFER_RATE'], alpha=0.5)
plt.title('Individual File Size vs Transfer Rate')
# plt.xlim(0,3000)
# plt.ylim(0, 15)
plt.xlabel('File Size (MiB)')
plt.ylabel('Transfer Rate (Gbps)')
plt.show()

# Calculate the rolling average of the transfer rate over a window of 100
rolling_avg = df[df['TYPE'] == 'file']['TRANSFER_RATE'].rolling(window=10).mean()

# Plotting the rolling average transfer rate over the index
plt.figure(figsize=(10, 6))
plt.plot(rolling_avg)
plt.title('Rolling Average Transfer Rate Over Index (Window=10)')
# plt.xlim(0,3000)
# plt.ylim(0, 15)
plt.xlabel('Individual file transfers')
plt.ylabel('Transfer Rate (Gbps)')
plt.show()

plt.figure(figsize=(10, 6))
plt.hist(df['FILE_SIZE']/1024**2, bins=50, color='blue', alpha=0.7, edgecolor='black')
plt.title('All Files Size Distribution')
plt.xlabel('File Size (MiB)')
plt.ylabel('Frequency')
# plt.xlim(0, 10)
# plt.ylim(0, 200)
# plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.show()

plt.figure(figsize=(10, 6))
plt.hist(df['TRANSFER_RATE'], bins=50, color='blue', alpha=0.7, edgecolor='black')
plt.title('All Files Transfer Rate Distribution')
plt.xlabel('Transfer Rate (Gbps)')
plt.ylabel('Frequency')
# plt.xlim(0, 10)
# plt.ylim(0, 200)
# plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.show()

plt.figure(figsize=(10, 6))
plt.scatter(df[df['COMPRESSION_USED']]['FILE_SIZE'] / 1024**2, df[df['COMPRESSION_USED']]['TRANSFER_RATE'], alpha=0.5, colorizer='blue', label='With Compression')
plt.scatter(df[~df['COMPRESSION_USED']]['FILE_SIZE'] / 1024**2, df[~df['COMPRESSION_USED']]['TRANSFER_RATE'], alpha=0.5, color='orange', label='Without Compression')
plt.title('All Files Size vs Transfer Rate')
# plt.xlim(0,3000)
# plt.ylim(0, 15)
plt.xlabel('File Size (MiB)')
plt.ylabel('Transfer Rate (Gbps)')
plt.show()

# Calculate the rolling average of the transfer rate over a window of 100
rolling_avg_comp = df[df['COMPRESSION_USED']]['TRANSFER_RATE'].rolling(window=100).mean()
rolling_avg_no_comp = df[~df['COMPRESSION_USED']]['TRANSFER_RATE'].rolling(window=100).mean()

# Plotting the rolling average transfer rate over the index
plt.figure(figsize=(10, 6))
plt.plot(rolling_avg_comp, label='With Compression')
plt.plot(rolling_avg_no_comp, label='Without Compression')
plt.title('Rolling Average Transfer Rate Over Index (Window=100)')
# plt.xlim(0,3000)
# plt.ylim(0, 15)
plt.xlabel('All file transfers')
plt.ylabel('Transfer Rate (Gbps)')
plt.legend()
plt.show()

print(f'Non-overlapping uplaods: {df[~df['OVERLAP']]}')

client = Client()
print(client.dashboard_link)
df = df.sort_values(by='UPLOAD_START').reset_index(drop=True)
client.scatter(df)
print("sorted df")
print("from pandas df to dask ddf")
ddf = dd.from_pandas(df, npartitions=100)
# --- 1. Data Preparation ---
# Ensure datetime types for accurate calculations
# ddf['UPLOAD_START'] = dd.to_datetime(ddf['UPLOAD_START'])
# ddf['UPLOAD_END'] = dd.to_datetime(ddf['UPLOAD_END'])

# Sort by upload start time to process events chronologically
# --- 2. Create an Event Timeline ---
# Create a list of "events": the start and end of each transfer.
# At each start, the total network rate increases. At each end, it decreases.
def gen_events(row):
    events = []
    if pd.notna(row['TRANSFER_RATE']) and row['TRANSFER_RATE'] > 0:
        events.append({'time': row['UPLOAD_START'], 'rate_change': row['TRANSFER_RATE'], 'compression_used': row['COMPRESSION_USED']})
        events.append({'time': row['UPLOAD_END'], 'rate_change': -row['TRANSFER_RATE'], 'compression_used': row['COMPRESSION_USED']})
    # return pd.DataFrame(events).sort_values(by='time').reset_index(drop=True)
    return events

def process_partition(partition):
    # Apply the function to each row to get a list of lists of dictionaries
    list_of_events = partition.apply(gen_events, axis=1).sum()
    # If no events were generated in this partition, return an empty DataFrame
    if not list_of_events:
        return pd.DataFrame({
            'time': pd.Series(dtype='datetime64[ns]'),
            'rate_change': pd.Series(dtype='float64'),
            'compression_used': pd.Series(dtype='bool')
        })
    # Convert the list of events into a single DataFrame for the partition
    return pd.DataFrame(list_of_events)

events_ddf = ddf.map_partitions(
    process_partition,
    meta={'time': 'datetime64[ns]', 'rate_change': 'float64', 'compression_used': 'bool'}
).dropna().reset_index(drop=True)
print("set up events_ddf")
# Convert the list of events to a DataFrame and sort chronologically
events_df = events_ddf.compute().sort_values(by='time').reset_index(drop=True)
events_df['time'] = pd.to_datetime(events_df['time'])
print("computed events_df")
# --- 3. Calculate Throughput Over Time ---
# Iterate through the timeline, calculating the total data transferred during each interval
# between events. During each interval, the total transfer rate is constant.
total_data_transferred_gb = 0
total_data_transferred_gb_comp = 0
total_data_transferred_gb_no_comp = 0
current_rate_gbps = 0
print("starting loop over events_df")
for i in range(len(events_df) - 1):
    # Update the current total rate with the change from the current event
    current_rate_gbps += events_df.loc[i, 'rate_change']

    # Calculate the duration of the current interval (until the next event)
    start_interval = events_df.loc[i, 'time']
    end_interval = events_df.loc[i + 1, 'time']
    duration_seconds = (end_interval - start_interval).total_seconds()

    # If there is time between events, calculate data transferred and add to total
    if duration_seconds > 0:
        # Data = Rate (Gbps) * Time (s) -> gives Gigabits
        data_in_interval_gb = current_rate_gbps * duration_seconds
        if events_df.loc[i, 'compression_used']:
            total_data_transferred_gb_comp += data_in_interval_gb
        else:
            total_data_transferred_gb_no_comp += data_in_interval_gb
        total_data_transferred_gb += data_in_interval_gb

# --- 4. Calculate Final Results ---
# Total time from the first upload start to the last upload end
total_duration_seconds = (events_df['time'].max() - events_df['time'].min()).total_seconds()
total_duration_seconds_comp = (events_df[events_df['compression_used']]['time'].max() - events_df[events_df['compression_used']]['time'].min()).total_seconds()
total_duration_seconds_no_comp = (events_df[~events_df['compression_used']]['time'].max() - events_df[~events_df['compression_used']]['time'].min()).total_seconds()

# The overall average network speed is the total data transferred divided by the total time
if total_duration_seconds_comp > 0:
    estimated_total_speed_gbps_comp = total_data_transferred_gb_comp / total_duration_seconds_comp
else:
    estimated_total_speed_gbps_comp = 0
if total_duration_seconds_no_comp > 0:
    estimated_total_speed_gbps_no_comp = total_data_transferred_gb_no_comp / total_duration_seconds_no_comp
else:
    estimated_total_speed_gbps_no_comp = 0
if total_duration_seconds > 0:
    estimated_total_speed_gbps = total_data_transferred_gb / total_duration_seconds
else:
    estimated_total_speed_gbps = 0

print(f"Total duration of all transfers (with compression): {total_duration_seconds_comp:.2f} seconds")
print(f"Total data transferred (with compression, accounting for overlaps): {total_data_transferred_gb_comp / 8:.2f} GiB")
print(f"Estimated average total network speed (with compression): {estimated_total_speed_gbps_comp:.2f} Gbps")
print(f"Total duration of all transfers (without compression): {total_duration_seconds_no_comp:.2f} seconds")
print(f"Total data transferred (without compression, accounting for overlaps): {total_data_transferred_gb_no_comp / 8:.2f} GiB")
print(f"Estimated average total network speed (without compression): {estimated_total_speed_gbps_no_comp:.2f} Gbps")
print(f"Total duration of all transfers (with or without compression): {total_duration_seconds:.2f} seconds")
print(f"Total data transferred (with or without compression, accounting for overlaps): {total_data_transferred_gb / 8:.2f} GiB")
print(f"Estimated average total network speed (with or without compression): {estimated_total_speed_gbps:.2f} Gbps")


# In[89]:


# Verify average
print(f"{(df[df['COMPRESSION_USED']]['UPLOAD_END'].max() - df[df['COMPRESSION_USED']]['UPLOAD_START'].min()).total_seconds():.2f} s, {df[df['COMPRESSION_USED']]['FILE_SIZE'].sum() / (1024**3):.2f} GiB, {df[df['COMPRESSION_USED']]['FILE_SIZE'].sum() * 8 / (1024**3) / ((df[df['COMPRESSION_USED']]['UPLOAD_END'].max() - df[df['COMPRESSION_USED']]['UPLOAD_START'].min()).total_seconds()):.2f} Gbps")
print(f"{(df[~df['COMPRESSION_USED']]['UPLOAD_END'].max() - df[~df['COMPRESSION_USED']]['UPLOAD_START'].min()).total_seconds():.2f} s, {df[~df['COMPRESSION_USED']]['FILE_SIZE'].sum() / (1024**3):.2f} GiB, {df[~df['COMPRESSION_USED']]['FILE_SIZE'].sum() * 8 / (1024**3) / ((df[~df['COMPRESSION_USED']]['UPLOAD_END'].max() - df[~df['COMPRESSION_USED']]['UPLOAD_START'].min()).total_seconds()):.2f} Gbps")
print(f"{(df['UPLOAD_END'].max() - df['UPLOAD_START'].min()).total_seconds():.2f} s, {df['FILE_SIZE'].sum() / (1024**3):.2f} GiB, {df['FILE_SIZE'].sum() * 8 / (1024**3) / ((df['UPLOAD_END'].max() - df['UPLOAD_START'].min()).total_seconds()):.2f} Gbps")


# In[90]:


# --- 5. Plot Total Throughput Over Time by Compression ---

# Helper function to generate step plot data from an events dataframe
def prepare_step_plot_data(df):
    if df.empty:
        return [], []

    # Calculate the cumulative sum of rate changes to get the total network speed
    df['total_rate'] = df['rate_change'].cumsum()

    # Prepare data for a step plot
    times = []
    rates = []

    # Start the plot at a rate of 0 just before the first event
    times.append(df['time'].iloc[0])
    rates.append(0)

    for i in range(len(df) - 1):
        # Point at the beginning of the interval with the new rate
        times.append(df['time'].iloc[i])
        rates.append(df['total_rate'].iloc[i])
        # Point at the end of the interval with the same rate (creates the horizontal step)
        times.append(df['time'].iloc[i+1])
        rates.append(df['total_rate'].iloc[i])

    # Add the final point
    times.append(df['time'].iloc[-1])
    rates.append(df['total_rate'].iloc[-1])

    return times, rates

# Separate events by compression
comp_events_df = events_df[events_df['compression_used']].copy().reset_index(drop=True)
nocomp_events_df = events_df[~events_df['compression_used']].copy().reset_index(drop=True)

# Generate plot data for each case
comp_plot_times, comp_plot_rates = prepare_step_plot_data(comp_events_df)
nocomp_plot_times, nocomp_plot_rates = prepare_step_plot_data(nocomp_events_df)

# --- Plotting the data ---
plt.figure(figsize=(15, 7))

# Plot for "With Compression"
if comp_plot_times:
    plt.plot(comp_plot_times, comp_plot_rates, label='With Compression', color='blue')
    plt.fill_between(comp_plot_times, comp_plot_rates, alpha=0.2, color='blue')

# Plot for "Without Compression"
if nocomp_plot_times:
    plt.plot(nocomp_plot_times, nocomp_plot_rates, label='Without Compression', color='orange')
    plt.fill_between(nocomp_plot_times, nocomp_plot_rates, alpha=0.2, color='orange')

plt.title('Total Network Upload Speed vs. Time')
plt.xlabel('Time')
plt.ylabel('Total Transfer Speed (Gbps)')
plt.grid(True, which='both', linestyle='--', linewidth=0.5)
plt.legend()
plt.show()


print(events_df)

all_comp = False

# To perform a time-based rolling average, we first set the 'time' column as the index for each dataset.
# The dataframes comp_events_df and nocomp_events_df were created in the previous cell.
comp_events_indexed_by_time = comp_events_df.set_index('time')
if not all_comp:
    nocomp_events_indexed_by_time = nocomp_events_df.set_index('time')

# Calculate the cumulative sum of rate changes to get the total network speed
comp_events_indexed_by_time['total_rate'] = comp_events_indexed_by_time['rate_change'].cumsum()
if not all_comp:
    nocomp_events_indexed_by_time['total_rate'] = nocomp_events_indexed_by_time['rate_change'].cumsum()

# Calculate the rolling mean over a 1-hour window for both datasets.
smoothed_rate_comp = comp_events_indexed_by_time['total_rate'].rolling('1h').mean()
if not all_comp:
    smoothed_rate_nocomp = nocomp_events_indexed_by_time['total_rate'].rolling('1h').mean()

# --- Convert x-axis from datetime to elapsed seconds for all plots ---
# Get the global start time of the first event
start_time = events_df['time'].min()

# Convert the datetime values for the original step plots to elapsed seconds
comp_plot_times_hours = [(t - start_time).total_seconds()/3600 for t in comp_plot_times]
if not all_comp:
    nocomp_plot_times_hours = [(t - start_time).total_seconds()/3600 for t in nocomp_plot_times]

# Convert the datetime index of the smoothed data to elapsed seconds
smoothed_times_comp_hours = (smoothed_rate_comp.index - start_time).total_seconds() / 3600
if not all_comp:
    smoothed_times_nocomp_hours = (smoothed_rate_nocomp.index - start_time).total_seconds() / 3600

# --- Plotting the original and smoothed data together ---
plt.figure(figsize=(15, 7))

# Plot for "With Compression"
if comp_plot_times:
    # Plot the original, "spiky" step plot
    plt.plot(comp_plot_times_hours, comp_plot_rates, label='Original Speed (With Compression)', alpha=0.4, color='blue')
    plt.fill_between(comp_plot_times_hours, comp_plot_rates, alpha=0.1, color='blue')
    # Plot the smoothed data
    plt.plot(smoothed_times_comp_hours, smoothed_rate_comp.values, label='1h Rolling Average (With Compression)', color='darkblue', linewidth=2)

# Plot for "Without Compression"
if nocomp_plot_times:
    # Plot the original, "spiky" step plot
    plt.plot(nocomp_plot_times_hours, nocomp_plot_rates, label='Original Speed (Without Compression)', alpha=0.4, color='orange')
    plt.fill_between(nocomp_plot_times_hours, nocomp_plot_rates, alpha=0.1, color='orange')
    # Plot the smoothed data
    plt.plot(smoothed_times_nocomp_hours, smoothed_rate_nocomp.values, label='1h Rolling Average (Without Compression)', color='darkred', linewidth=2)


plt.title(f'Total and Smoothed Network Upload Speed vs. Time\n{platform.capitalize()} - {compression} - {workers}-{threads}')
plt.xlabel('Time (hours since first upload)')
plt.ylabel('Total Transfer Speed (Gbps)')
plt.grid(True, which='both', linestyle='--', linewidth=0.5)
plt.legend()
plt.tight_layout()

plt.savefig(os.path.expanduser(f'{this_base_path}/total_and_smoothed_network_upload_speed_{platform}_{compression}_{workers}-{threads}.png'))
plt.show()

# --- 7. Analyze and Plot Upload Concurrency by Compression ---

# Create a figure with two subplots, sharing the Y-axis for easier comparison
fig, axes = plt.subplots(1, 2, figsize=(18, 6), sharey=True)
fig.suptitle('Upload Concurrency Analysis', fontsize=16)

# --- Plot for "With Compression" ---
comp_events_df = events_df[events_df['compression_used'] == True].copy()
if not comp_events_df.empty:
    # Determine the change in concurrency at each event (+1 for start, -1 for end)
    comp_events_df['concurrency_change'] = comp_events_df['rate_change'].apply(lambda x: 1 if x > 0 else -1)
    # Calculate the cumulative sum to find the number of concurrent uploads at any given time
    comp_events_df['concurrency'] = comp_events_df['concurrency_change'].cumsum()

    # Plot a histogram of the concurrency counts
    max_concurrency_comp = int(comp_events_df['concurrency'].max())
    axes[0].hist(comp_events_df['concurrency'], bins=range(0, max_concurrency_comp + 2), align='left', rwidth=0.8, edgecolor='black', color='blue')
    axes[0].set_title('With Compression')
    axes[0].set_xlabel('Number of Concurrent Uploads')
    axes[0].set_ylabel('Frequency (Time Spent at this Concurrency Level)')
    axes[0].set_xlim(-1, 25)
    axes[0].set_xticks(np.arange(0, 25))
    axes[0].grid(axis='y', linestyle='--', alpha=0.7)

# --- Plot for "Without Compression" ---
nocomp_events_df = events_df[events_df['compression_used'] == False].copy()
if not nocomp_events_df.empty:
    # Determine the change in concurrency at each event (+1 for start, -1 for end)
    nocomp_events_df['concurrency_change'] = nocomp_events_df['rate_change'].apply(lambda x: 1 if x > 0 else -1)
    # Calculate the cumulative sum to find the number of concurrent uploads at any given time
    nocomp_events_df['concurrency'] = nocomp_events_df['concurrency_change'].cumsum()

    # Plot a histogram of the concurrency counts
    max_concurrency_nocomp = int(nocomp_events_df['concurrency'].max())
    axes[1].hist(nocomp_events_df['concurrency'], bins=range(0, max_concurrency_nocomp + 2), align='left', rwidth=0.8, edgecolor='black', color='orange')
    axes[1].set_title('Without Compression')
    axes[1].set_xlabel('Number of Concurrent Uploads')
    axes[1].set_xlim(-1, 25)
    axes[1].set_xticks(np.arange(0, 25))
    axes[1].grid(axis='y', linestyle='--', alpha=0.7)

plt.tight_layout(rect=[0, 0.03, 1, 0.95]) # Adjust layout to make room for suptitle
plt.savefig(os.path.expanduser(f'{this_base_path}/upload_concurrency_{platform}_{compression}_{workers}-{threads}.png'))

print(events_df)

