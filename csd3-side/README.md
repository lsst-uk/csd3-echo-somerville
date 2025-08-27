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

Copy [`distributed.yaml`](distributed.yaml) to `~/.config/dask`.
