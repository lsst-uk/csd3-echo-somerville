FROM ubuntu

LABEL org.opencontainers.image.source https://github.com/lsst-uk/csd3-echo-somerville
LABEL org.opencontainers.image.description="Code to backup and curate LSST-UK data from CSD3 to Echo."
LABEL org.opencontainers.image.licenses="Apache-2.0"

RUN apt-get update && apt-get install -y git

RUN git clone https://github.com/lsst-uk/csd3-echo-somerville.git
