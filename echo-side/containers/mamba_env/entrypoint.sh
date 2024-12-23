#!/bin/bash

eval "$(micromamba shell hook --shell bash)"

source ~/.bashrc

micromamba activate lsst-uk

$*