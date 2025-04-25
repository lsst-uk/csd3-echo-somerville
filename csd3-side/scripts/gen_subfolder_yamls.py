# !/usr/bin/env python
# coding: utf-8
# D.McKay Apr 2025

# Imports
import os
import sys
import argparse
import yaml


def gen_config(
    bucket_name, api, subfolder_local_dir, prefix, S3_folder, folder,
    subfolder_nprocs, subfolder_threads_per_worker, no_collate, upload_dryrun,
    no_compression, no_file_count_stop, exclude
):
    return {
        'bucket_name': bucket_name,
        'api': api,
        'local_path': subfolder_local_dir,
        'S3_prefix': prefix,
        'S3_folder': os.path.join(S3_folder, folder) if S3_folder else folder,
        'nprocs': subfolder_nprocs,
        'threads_per_process': subfolder_threads_per_worker,
        'no_collate': no_collate,
        'dryrun': upload_dryrun,
        'no_compression': no_compression,
        'no_file_count_stop': no_file_count_stop,
        'exclude': exclude,
    }


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
        help='Top-level YAML configuration file from which subfolder config files will be generated.',
        required=True,
    )
    parser.add_argument(
        '--target-parallelism',
        type=int,
        default=4,
        help='The ideal number of jobs to run in parallel - CPUs will be divided by this to yield '
        'max number of CPUs per job.',
        required=True,
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

    if api not in ['s3', 'swift']:
        sys.exit('API must be "S3" or "Swift" (case insensitive).')

    if not os.path.exists(local_dir):
        sys.exit(f'Local path {local_dir} does not exist.')
    elif not os.path.isdir(local_dir) or not os.access(local_dir, os.R_OK):
        sys.exit(f'Local path {local_dir} is not a directory or is not readable.')
    elif os.path.abspath(local_dir) != local_dir:
        sys.exit(f'Local path {local_dir} is not an absolute path.')
    else:
        contents = os.listdir(local_dir)
        print(f'Local path {local_dir} contains:')
        print(contents)

    folder_list = [f for f in contents if os.path.isdir(os.path.join(local_dir, f))]
    if len(folder_list) == 0:
        sys.exit(f'Local path {local_dir} does not contain any subfolders.')
    elif len(folder_list) == 1:
        sys.exit(f'Local path {local_dir} contains only one subfolder: {folder_list[0]}.\n'
                 'Please provide a top-level folder that contains subfolders or upload '
                 f'{folder_list[0]} directly.')
    elif len(folder_list) == len(contents):
        print('All contents of the local path are subfolders.')
        print('After subfolders are uploaded, there will be no need to upload the top-level folder.')
    else:
        print('Some contents of the local path are files.')
        print('After subfolders are uploaded, the top-level folder will need to be uploaded '
              '(excluding subfolders).')

    print(f'Top-level local path {local_dir}')

    print(f'Top-level S3_folder {S3_folder if len(S3_folder) > 0 else "None"}')

    print(f'Total number of processes: {nprocs}')
    print(f'Total threads per workers: {threads_per_worker}')
    print('An attempt will be made to divide the number of subfolders evenly amoungst processors '
          'and threads.')

    dryrun = args.dryrun

    top_level_config = {
        'bucket_name': bucket_name,
        'api': api,
        'local_path': local_dir,
        'S3_prefix': prefix,
        'S3_folder': S3_folder,
        'nprocs': nprocs,
        'threads_per_process': threads_per_worker,
        'no_collate': no_collate,
        'dryrun': upload_dryrun,
        'no_compression': no_compression,
        'no_file_count_stop': no_file_count_stop,
        'exclude': exclude,
    }
    print(f'Top-level config:\n{top_level_config}')
    num_subfolders = len(folder_list)
    print(f'Number of subfolders: {num_subfolders}')

    target_parallelism = args.target_parallelism
    if target_parallelism < 1:
        sys.exit('Target parallelism must be greater than 0.')
    elif target_parallelism > nprocs:
        sys.exit(f'Target parallelism {target_parallelism} exceeds the number of processors {nprocs}.')
    else:
        print(f'Target parallelism: {target_parallelism}')
    max_nprocs_per_job = nprocs // target_parallelism
    print(f'Maximum number of processes per job: {max_nprocs_per_job}')

    threading_proportion = nprocs // threads_per_worker
    # Calculate the number of threads per worker
    subfolder_threads_per_worker = max(1, max_nprocs_per_job // threading_proportion)
    print(f'Each subfolder job will use nprocs = {max_nprocs_per_job}')
    print(f'Each subfolder will use threads_per_worker = {subfolder_threads_per_worker}')
    if subfolder_threads_per_worker > 1:
        print(f'Target parallelism of {target_parallelism} and '
              f'threading proportion of {threading_proportion} is achievable.')
    else:
        print(f'Target parallelism of {target_parallelism} and threading '
              f'proportion of {threading_proportion} is not achievable.\n'
              'Threads per worker reduced to 1.')

    # if max_nprocs_per_job * subfolder_threads_per_worker * num_subfolders > nprocs:
    #     print('Warning: The total number of processes and threads for all subfolders\n'
    #           '(procs * threads * folders = '
    #           f'{subfolder_nprocs} * {subfolder_threads_per_worker} * {num_subfolders} = '
    #           f'{subfolder_nprocs * subfolder_threads_per_worker * num_subfolders}) '
    #           f'exceeds the total available number of processors specified {nprocs}.\n'
    #           'This may lead to suboptimal performance if all are run concurrently.')
    print('\n------------------------------------------------------------------------')
    print('Subfolder config files will be created in the current working directory.')
    print('------------------------------------------------------------------------')

    # Create the subfolders and config files
    for folder in folder_list:
        working_subfolder = os.path.join(os.getcwd(), folder)
        subfolder_config_file = os.path.join(working_subfolder, 'config.yaml')
        subfolder_local_dir = os.path.join(local_dir, folder)
        subfolder_config = gen_config(
            bucket_name,
            api,
            subfolder_local_dir,
            prefix,
            S3_folder,
            folder,
            max_nprocs_per_job,
            subfolder_threads_per_worker,
            no_collate,
            upload_dryrun,
            no_compression,
            no_file_count_stop,
            exclude
        )
        # Write the subfolder config to a YAML file
        if not dryrun:
            os.makedirs(working_subfolder, exist_ok=True)
            with open(subfolder_config_file, 'w') as f:
                yaml.dump(
                    subfolder_config,
                    f
                )
            print(f'Created subfolder: {working_subfolder}')
            print(f'Created config file: {subfolder_config_file}')
        else:
            print(f'Would create subfolder: {working_subfolder}')
            print(f'Would create config file: {subfolder_config_file}')
        print(f'Subfolder config:\n{subfolder_config}')
        print('----------------------------------------')
    print('All subfolder config files created.')
    print('If you are happy with the config files, run:')
    print(f'echo {" ".join(folder_list)} | sed "s/ /\\n/g" | xargs -n 1 -P {target_parallelism} '
          '-I name bash -c '
          '"cd name; python $CES_HOME/csd3-side/scripts/lsst-backup.py --config-file config.yaml '
          '> name.log 2> name.err" &')
    print('----------------------------------------')
