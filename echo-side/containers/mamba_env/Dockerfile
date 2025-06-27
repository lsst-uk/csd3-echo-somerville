FROM mambaorg/micromamba

LABEL org.opencontainers.image.source https://github.com/lsst-uk/csd3-echo-somerville
LABEL org.opencontainers.image.description="Code to backup and curate LSST-UK data from CSD3 to Echo."
LABEL org.opencontainers.image.licenses="Apache-2.0"


COPY environment.yaml /environment.yaml
USER root
RUN apt-get update && apt-get install -y git kubectl
RUN mkdir -p /airflow/xcom
RUN chown -R mambauser:mambauser /airflow
USER mambauser
RUN micromamba --version

RUN micromamba env create --name lsst-uk --file=/environment.yaml

RUN echo "source activate lsst-uk" >> ~/.bashrc

RUN echo "alias mamba='micromamba'" >> ~/.bashrc

SHELL ["micromamba", "run", "-n", "lsst-uk", "/bin/bash", "-c"]

RUN git clone https://github.com/lsst-uk/csd3-echo-somerville.git

RUN cd csd3-echo-somerville && python -m pip install .

COPY echo-side/containers/mamba_env/entrypoint.sh /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
