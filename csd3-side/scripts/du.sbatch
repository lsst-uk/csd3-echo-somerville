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
top_path=/rds/project/rds-rPTGgs6He74/ras81/lsst-ir-fusion/dmu5
paths=( $(find $top_path -maxdepth 1 -type d) )

for path in "${paths[@]:1}" # ignore index zero as it will be '.'
do
	cd $path
	date
	pwd -P
	find . -print0 | xargs -P 36 -0 du -L 2>/dev/null| awk '{SUM+=$1} END {print SUM/1048576}'
	echo gibibytes
	date
done
