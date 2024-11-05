#!/bin/bash
sf_list=$1
for sf in $(cat $sf_list);
do
        echo $sf
done