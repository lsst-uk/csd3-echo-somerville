FROM continuumio/miniconda3

RUN conda install -y -c conda-forge mamba
RUN mamba init && mamba install -y boto 

COPY bucket_manager.py .
COPY monitor_s3.py .
COPY entrypoint.sh .

ENTRYPOINT ["./entrypoint.sh","$*"]
