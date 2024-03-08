#!/bin/bash -login
#SBATCH -A IRIS-IP005-CPU
#SBATCH --job-name="volume"
#SBATCH --time=12:00:00
#SBATCH --partition=cclake
#SBATCH --nodes=1
#SBATCH --tasks-per-node=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=3420mb

source $HOME/.bashrc
mamba activate

cd /home/ir-mcka1/csd3-echo-somerville/csd3-side

python parse_butler_yaml_to_paths.py /rds/project/rds-lT5YGmtKack/ras81/butler_full_20221201 exports_deepCoadd_calexp_viking_20230525.yaml
