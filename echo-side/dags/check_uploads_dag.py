###
# def get_new_csvs(bucket_name):
#     new_csvs[bucket_name] = []
#     with open(''.join([f'/lsst-backup-logs/new-csv-files-{bucket_name}','{{ ds_nodash }}','.txt']), 'r') as f:
#         for line in f:
#             new_csvs[bucket_name].append(line.strip())
#     if len(new_csvs[bucket_name]) == 0:
#         del new_csvs[bucket_name]


    # get_new_csvs_task = [ PythonOperator(
    #     task_id=f'get_new_csvs-{bucket_name}',
    #     python_callable=get_new_csvs,
    #     op_args=[bucket_name],
    # ) for bucket_name in bucket_names ]

        # get_new_csvs_task,

###



###
# check_uploads = []

#     for bucket_new_csvs in new_csvs.items():
#         bucket_name = bucket_new_csvs[0]
#         new_csvs_list = bucket_new_csvs[1]
#         for csv in new_csvs_list:
#             check_uploads.append(
#                 KubernetesPodOperator(
#                 task_id=f'check_{csv}',
#                 image='ghcr.io/lsst-uk/csd3-echo-somerville:latest',
#                 cmds=['./entrypoint.sh'],
#                 arguments=['python', 'csd3-echo-somerville/scripts/check_upload.py', bucket_name, csv],
#                 env_vars={
#                     'ECHO_S3_ACCESS_KEY': Variable.get("ECHO_S3_ACCESS_KEY"),
#                     'ECHO_S3_SECRET_KEY': Variable.get("ECHO_S3_SECRET_KEY"),
#                 },
#                 volumes=[logs_volume],
#                 volume_mounts=[logs_volume_mount],
#                 get_logs=True,
#                 )
#             )
###