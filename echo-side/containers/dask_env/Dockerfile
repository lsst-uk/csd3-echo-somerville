FROM ghcr.io/dask/dask:latest

LABEL org.opencontainers.image.source https://github.com/lsst-uk/csd3-echo-somerville
LABEL org.opencontainers.image.description="Extension to Dask to provide additional libraries to workers at runtime."
LABEL org.opencontainers.image.licenses="Apache-2.0"

COPY environment.yaml /environment.yaml

RUN conda -c conda-forge env update --name base --file /environment.yaml && \
    conda clean -afy && \
    rm -rf /opt/conda/pkgs/*

RUN echo "alias mamba='conda'" >> ~/.bashrc

RUN git clone --branch 128-clean-up-zips-with-dask-k8s https://github.com/lsst-uk/csd3-echo-somerville.git

RUN cd csd3-echo-somerville && python -m pip install .