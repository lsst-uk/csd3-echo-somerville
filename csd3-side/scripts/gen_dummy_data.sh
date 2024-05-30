#!/bin/bash
# number of folders
n=150
# number of files per folder
f=1
# size of each file
s=34

usage() {
  echo "Usage: $0 [-n number_of_folders] [-f number_of_files_per_folder] [-s size_of_each_file]"
  echo "Default values: n=75, f=2, s=34"
  exit 1
}

while getopts "n:f:s:" opt; do
  case ${opt} in
    n ) # process option n
      n=$OPTARG
      ;;
    f ) # process option f
      f=$OPTARG
      ;;
    s ) # process option s
      s=$OPTARG
      ;;
    \? ) usage
      ;;
  esac
done

for i in $(seq 1 $n)
do
    mkdir $i
    for j in $(seq 1 $f)
    do
        dd if=/dev/zero of=$i/${i}_${j}.f bs=1M count=$s &
    done
done