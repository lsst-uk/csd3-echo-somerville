#!/usr/bin/env python


from bucket_manager import bucket_manager as bm
import swiftclient
from dask import dataframe as dd
from distributed import Client
import pandas as pd


def copy_object(row, conn, from_container, to_container):
    object_name = row['object']
    try:
        conn.copy_object(from_container, object_name, destination=f'/{to_container}/{object_name}')
    except swiftclient.ClientException as e:
        print(f"Error copying {object_name}: {e}")
        return False
    print(f"Copied {object_name} from {from_container} to {to_container}")
    return True


def delete_original(row, conn, container):
    object_name = row['object']
    if not row['copied']:
        print(f"Skipping deletion of {object_name} as copy failed")
        return False
    try:
        conn.delete_object(container, object_name)
    except swiftclient.ClientException as e:
        print(f"Error deleting {object_name}: {e}")
        return False
    print(f"Deleted {object_name} from {container}")
    return True


if __name__ == "__main__":

    try:
        bm.check_keys()
    except Exception as e:
        print("Error checking keys:", e)
        exit(1)

    conn = bm.get_conn_swift()

    # Edit these variables to match your needs
    from_container = 'LSST-IR-FUSION-Butlers'
    to_container = 'LSST-IR-FUSION-Butlers-wide'
    prefix = 'butler_wide_20220930'

    bm.bucket_list_swift(conn)
    use_existing = False

    try:
        assert from_container in bm.bucket_list_swift(
            conn
        ), f"Source container {from_container} does not exist, exiting."
    except AssertionError as e:
        print(e)
        exit(1)
    try:
        assert to_container not in bm.bucket_list_swift(
            conn
        ), f"Destination container {to_container} already exists."
    except AssertionError as e:
        print(e)
        print("Continue? (y/n)")
        response = input()
        if response.lower() != 'y':
            exit(1)
        else:
            use_existing = True

    if not use_existing:
        bm.create_bucket_swift(conn, to_container)

    from_objects = bm.object_list_swift(
        conn,
        from_container,
        prefix=prefix,
        full_listing=True
    )
    num_existing = len(from_objects)
    print(f'Existing objects: {num_existing}')
    print(f'Copying objects from {from_container} to {to_container}...')

    with Client() as client:
        ddf = dd.from_pandas(
            pd.DataFrame(
                from_objects,
                columns=['object']
            ),
            npartitions=num_existing // len(client.scheduler_info()['workers'])
        )

        ddf['copied'] = ddf.map_partitions(
            lambda partition: partition.apply(
                copy_object,
                axis=1,
                args=(conn, from_container, to_container),
            ),
            meta=('copied', 'bool')
        )

        # if all copied, delete the original object
        ddf['deleted_original'] = ddf.map_partitions(
            lambda partition: partition.apply(
                delete_original,
                axis=1,
                args=(conn, from_container)
            ),
            meta=('deleted_original', 'bool')
        )

        ddf.to_csv('copy_results.csv', single_file=True, index=False)
        print("Copying completed. Results saved to copy_results.csv")
        print("Deleting original objects completed. Results saved to copy_results.csv")
        print("All operations completed.")
