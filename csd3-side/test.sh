#!/bin/bash
paths=( $(find /rds/project/rds-rPTGgs6He74/ras81/lsst-ir-fusion/dmu4/ -maxdepth 1 -type d) )
for path in "${paths[@]:1}"
do
	echo $path
done
