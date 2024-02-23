#!/bin/bash -login
#SBATCH -A IRIS-IP005-CPU
#SBATCH --job-name="volume"
#SBATCH --time=12:00:00
#SBATCH --partition=cclake
#SBATCH --nodes=1
#SBATCH --tasks-per-node=1
#SBATCH --cpus-per-task=36
#SBATCH --mem=3420mb

# Change to job working directory
cd /rds/project/rds-rPTGgs6He74/ras81/lsst-ir-fusion/dmu4
date
pwd -P
find . -type f -print0 | xargs -P 36 -0 du | awk '{SUM+=$1} END {print SUM/1048576}'
date
