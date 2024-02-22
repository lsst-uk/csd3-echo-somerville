#!/bin/bash -login
#SBATCH -A IRIS-IP005-CPU
#SBATCH --job-name="volume"
#SBATCH --time=06:00:00
#SBATCH --partition=cclake
#SBATCH --nodes=1
#SBATCH --tasks-per-node=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=3420mb

# Change to job working directory
cd /rds/project/rds-lT5YGmtKack
date
pwd -P
du -sh ras81
date
cd /rds/project/rds-rPTGgs6He74
pwd -P
du -sh ras81
date
