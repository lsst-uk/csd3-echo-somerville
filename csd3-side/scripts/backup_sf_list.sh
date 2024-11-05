#!/bin/bash
sf_listi_path=$1
template_path=$2
for sf in $(cat $sf_list_path);
do
        mkdir $sf
        cd $sf
        sed "s/SUBFOLDER/$sf/g" $template_path > $sf.yaml
        date >> ${sf}_timings.log
        python ../../../scripts/lsst-backup.py --config-file $sf.yaml > $sf.log 2> $sf.err
        date >> ${sf}_timings.log
done