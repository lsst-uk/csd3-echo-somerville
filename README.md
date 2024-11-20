# csd3-echo-somerville

[![License](https://img.shields.io/badge/License-Apache_2.0-green.svg)](https://opensource.org/licenses/Apache-2.0)

Code and VM recipes for backup from CSD3 to Echo S3, curate at STFC cloud and expose to Somerville.

Main aim to move scientifically valuable intermediate and output data from DEV activities from Cambridge [CSD3](https://docs.hpc.cam.ac.uk/hpc/) to RAL [Echo](https://iopscience.iop.org/article/10.1088/1742-6596/898/6/062051/pdf) to free up workspace. Data will be curated and made available to users of [Somerville](https://www.ed.ac.uk/information-services/research-support/research-computing/ecdf). This project is further documented on LSST:UK Confluence.

## Installation

Installs the contents of the "bucket_manager" folder as a python package.

```shell
python -m pip install .
```

## Use

"Scripts" contains general scripts that are runnable from anywhere.

"\<location\>-side" folders contain scripts and Jupyter notebooks intended to be run in specific locations.
.

## Building containers

e.g., from root folder:

```shell
docker build -t ghcr.io/lsst-uk/csd3-somerville-echo-basic:v0.1.0 -f echo-side/containers/basic_env/Dockerfile .
```

## Running containers

e.g., to list current bucket, with environemnt variables for S3 access set on the host:

```shell
docker run \
-e ECHO_S3_ACCESS_KEY=$S3_ACCESS_KEY \
-e ECHO_S3_SECRET_KEY=$S3_SECRET_KEY \
ghcr.io/lsst-uk/csd3-echo-somerville:latest python csd3-echo-somerville/scripts/list_buckets.py
```
