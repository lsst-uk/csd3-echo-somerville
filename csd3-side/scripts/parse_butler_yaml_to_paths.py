#!/usr/env/python 
#Encoding UTF-8
#D. McKay Feb 2024
#Script to parse a Butler export YAML file for paths and create a list of all paths, with any symlinks resolved

import os
import sys
from tqdm import tqdm
import pandas as pd

butler_base_dir = sys.argv[1] #'/rds/project/rds-lT5YGmtKack/ras81/butler_wide_20220930'
butler_data_dir = f'{butler_base_dir}/data'
butler_export_path = f'{butler_base_dir}/{sys.argv[2]}' # exports_deepCoadd_calexp_video_20230525.yaml' # smallest butler export yaml file

#Note Butler export YAML files contain non-standard YAML flags.
#We just need paths, so simply parse for paths.
print('Parsing Butler export file.')
with open(butler_export_path,'r') as butler_export:	
	paths = [f'{butler_data_dir}/{p.strip().split()[-1]}' for p in tqdm(butler_export.readlines()) if p.strip().startswith('path:')]

#Paths in paths can be paths to files or to symbolic links
print('Resolving paths')
real_paths = [os.path.realpath(p) if os.path.islink(p) else p for p in tqdm(paths)]

#Write to CSV
csv_path = f'{os.getcwd()}/{butler_export_path.split("/")[-1][:-5]}_paths.csv'
df = pd.DataFrame.from_dict({'butler_path':paths,'real_path':real_paths},dtype=str)
print(f'Done. {len(df)} paths found, of which {len(df.query("butler_path != real_path"))} were symlinks.')
print(df.head())
print('...')
print(df.tail())
print(f'Writing paths lists to {csv_path}.')
df.to_csv(csv_path,index=False)
