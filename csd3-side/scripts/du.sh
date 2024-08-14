#!/bin/bash

# Script to discover the size of datasets using a back-end node

#SBATCH -A IRIS-IP005-CPU
#SBATCH --job-name="volume"
#SBATCH --time=06:00:00
#SBATCH --partition=cclake
#SBATCH --nodes=1
#SBATCH --tasks-per-node=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=3420mb

# Change to job working directory
# Edit to parent folder of folder(s) of interest
cd /rds/project/rds-lT5YGmtKack/ras81/butler_wide_20220930/data/u/ir-shir1/DRP
date
pwd -P
# uncomment the below for multiple subfolders
du -h --max-depth=1
# uncomment the below for a single named folder
# du -sh folder
date

# for multiple named folders, copy/paste the above
