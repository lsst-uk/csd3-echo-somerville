# CSD3-side scripts

Develop code here to run on an CSD3 to put data to Echo S3.

Required secrets should be separately added to environment variables:
ECHO_S3_ACCESS_KEY
ECHO_S3_SECRET_KEY
ECHO_SWIFT_USER
ECHO_SWIFT_SECRET_KEY

e.g., if using boto3 S3 API scripts:

```shell
export ECHO_S3_ACCESS_KEY=$(grep access_key <S3 JSON file> | awk -F \" '{print $4}')
export ECHO_S3_SECRET_KEY=$(grep secret_key <S3 JSON file> | awk -F \" '{print $4}')
```

e.g., if using swiftclient Swift API scripts:

```shell
export ECHO_SWIFT_USER=$(grep user <SWIFT JSON file> | awk -F \" '{print $4}')
export ECHO_SWIFT_SECRET_KEY=$(grep secret_key <SWIFT JSON file> | awk -F \" '{print $4}')
```

## Configuration

Create a file in `~/.config/dask` called `/distributed.yaml` with the following text.

```yaml
distributed:
  worker:
   # Fractions of worker process memory at which we take action to avoid memory
   # blowup. Set any of the values to False to turn off the behavior entirely.
    memory:
      target: 0.80     # fraction of managed memory where we start spilling to disk
      spill: 0.95      # fraction of process memory where we start spilling to disk
      pause: 0.98      # fraction of process memory at which we pause worker threads
      terminate: 0.99  # fraction of process memory at which we terminate the worker
  temporary-directory: "/rds/project/rds-rPTGgs6He74/$USER/temp"
```
