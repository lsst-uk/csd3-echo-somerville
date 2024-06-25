FROM continuumio/miniconda3

LABEL org.opencontainers.image.source https://github.com/lsst-uk/csd3-echo-somerville
LABEL org.opencontainers.image.description="Code to backup and curate LSST-UK data from CSD3 to Echo."
LABEL org.opencontainers.image.licenses="Apache-2.0"

RUN conda install -y -c conda-forge mamba && mamba init

COPY environment.yaml /environment.yaml

RUN mamba env create --name lsst-uk --file=/environment.yaml

SHELL ["conda", "run", "-n", "lsst-uk", "/bin/bash", "-c"]

RUN git clone https://github.com/lsst-uk/csd3-echo-somerville.git

RUN cd csd3-echo-somerville && python -m pip install .

COPY echo-side/containers/basic_env/entrypoint.sh /entrypoint.sh

RUN mamba env export

ENTRYPOINT ["/entrypoint.sh"]
