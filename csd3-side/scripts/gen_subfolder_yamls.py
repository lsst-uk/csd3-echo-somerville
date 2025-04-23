# !/usr/bin/env python
# coding: utf-8
# D.McKay Apr 2025

# Imports
import os
import sys
import argparse
import yaml

##########################################
#              Main Function             #
##########################################
if __name__ == '__main__':
    epilog = ''

    class MyParser(argparse.ArgumentParser):
        def error(self, message):
            sys.stderr.write(f'error: {message}\n\n')
            self.print_help()
            sys.exit(2)
    parser = MyParser(
        description='Generate YAMLs and subfolders based on a top-level YAML, where the `local_dir` '
        'specified contains subfolders that will be run through `lsst-backup.py`, '
        'potentially in an embarrassingly parallel mode.\n'
        'The top-level YAML should have been generated via '
        '`lsst-backup.py --config-file <name.yaml> --save-config`.',
        epilog=epilog,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        '--config-file',
        type=str,
        help='Top-level YAML configuration file from which subfolder config files will be generated.'
    )
    parser.add_argument(
        '--dryrun',
        default=False,
        action='store_true',
        help='Perform a dry run without generating subfolders or config files.'
        '\nNote: this differs from the `--dryrun` option in the config files, '
        ' which is an argument to `lsst-backup.py`.'
    )
    args = parser.parse_args()

    if not args.config_file:
        sys.exit('A top-level config file must be provided.')
    else:
        config_file = args.config_file
        if not os.path.exists(config_file):
            sys.exit(f'Config file {config_file} does not exist.')
        else:
            with open(config_file, 'r') as f:
                config = yaml.safe_load(f)
                print(config)
                if 'bucket_name' in config.keys():
                    bucket_name = config['bucket_name']
                if 'local_path' in config.keys():
                    local_dir = config['local_path']
                if 'S3_prefix' in config.keys():
                    prefix = config['S3_prefix']
                if 'S3_folder' in config.keys():
                    S3_folder = config['S3_folder']
                if 'exclude' in config.keys():
                    exclude = config['exclude']
                if 'nprocs' in config.keys():
                    nprocs = config['nprocs']
                else:
                    nprocs = 48
                if 'threads_per_worker' in config.keys():
                    threads_per_worker = config['threads_per_worker']
                else:
                    threads_per_worker = 4
                if 'no_checksum' in config.keys():
                    no_checksum = config['no_checksum']
                else:
                    no_checksum = False
                if 'no_collate' in config.keys():
                    no_collate = config['no_collate']
                else:
                    no_collate = False
                if 'dryrun' in config.keys():
                    upload_dryrun = config['dryrun']
                else:
                    upload_dryrun = False
                if 'no_compression' in config.keys():
                    no_compression = config['no_compression']
                else:
                    no_compression = True
                if 'no_file_count_stop' in config.keys():
                    no_file_count_stop = config['no_file_count_stop']
                else:
                    no_file_count_stop = False
                if 'api' in config.keys():
                    api = config['api'].lower()
                else:
                    api = 'swift'
    save_config = False
    # no_checksum = args.no_checksum
    # if no_checksum:
    #     parser.error('Please note: the optin to disable file checksum has been deprecated.')

    # save_config = args.save_config
    # api = args.api.lower()
    if api not in ['s3', 'swift']:
        sys.exit('API must be "S3" or "Swift" (case insensitive).')
    # bucket_name = args.bucket_name
    # local_dir = args.local_path

    if not os.path.exists(local_dir):
        sys.exit(f'Local path {local_dir} does not exist.')

    print(f'Top-level local path {local_dir}')
    # prefix = args.S3_prefix
    # sub_dirs = args.S3_folder
    print(f'Top-level S3_folder {S3_folder if len(S3_folder) > 0 else "None"}')
    # nprocs = args.nprocs
    # threads_per_worker = args.threads_per_worker
    print(f'Total number of processes: {nprocs}')
    print(f'Total threads per workers: {threads_per_worker}')
    print('An attempt will be made to divide the number of subfolders evenly amoungst processors and threads.')
    # internally, flag turns *on* collate, but for user no-collate turns it
    # off - makes flag more intuitive
    # global_collate = not args.no_collate
    # upload_dryrun = dryrun
    dryrun = args.dryrun
    # internally, flag turns *on* compression, but for user no-compression
    # turns it off - makes flag more intuitive
    # use_compression = not args.no_compression
    # internally, flag turns *on* file-count-stop, but for user
    # no-file-count-stop turns it off - makes flag more intuitive
    # file_count_stop = not args.no_file_count_stop

    # if exclude:
    #     exclude = pd.Series(exclude)
    # else:
    #     exclude = pd.Series([])

    # print(f'Config: {args}')

    top_level_config = {
        'bucket_name': bucket_name,
        'api': api,
        'local_path': local_dir,
        'S3_prefix': prefix,
        'S3_folder': S3_folder,
        'nprocs': nprocs,
        'threads_per_process': threads_per_worker,
        'no_collate': no_collate,
        'dryrun': dryrun,
        'no_compression': no_compression,
        'no_file_count_stop': no_file_count_stop,
        'exclude': exclude,
    }
    print(f'Top-level config:\n{top_level_config}')

    # if save_config:
        # with open(config_file, 'w') as f:
        #     yaml.dump(
        #         {
        #             'bucket_name': bucket_name,
        #             'api': api,
        #             'local_path': local_dir,
        #             'S3_prefix': prefix,
        #             'S3_folder': S3_folder,
        #             'nprocs': nprocs,
        #             'threads_per_process': threads_per_worker,
        #             'no_collate': not global_collate,
        #             'dryrun': dryrun,
        #             'no_compression': not use_compression,
        #             'no_file_count_stop': not file_count_stop,
        #             'exclude': exclude.to_list(),
        #         },
        #         f)
        # sys.exit(0)
