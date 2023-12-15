import os
import traceback
import dask.dataframe as dd
from ftplib import FTP, error_perm

import paramiko
from src.main.a360_data_compute.crawler.service.dataframe_comparison import log_utility


def read_local_csv(table_path, delimiter, quote_char):
    try:
        if not table_path.lower().endswith('.csv'):
            global csv_files
            if os.path.isdir(table_path):
                csv_files = [
                    os.path.join(table_path, file)
                    for file in os.listdir(table_path)
                    if file.lower().endswith('.csv')
                ]
                if not csv_files:
                    raise FileNotFoundError(f"No CSV files found in the path: {table_path}")
            elif os.path.isfile(table_path) and table_path.lower().endswith('.csv'):
                csv_files = [table_path]
            else:
                raise FileNotFoundError(f"Invalid path or file not found: {table_path}")
        else:
            csv_files = table_path

        return dd.read_csv(csv_files, delimiter=delimiter, quotechar=quote_char, dtype='object')
    # validate here it is a concatinated df

    except Exception as e:
        error_msg = f"An unexpected error occurred reading csv in locally: {str(e)}"
        traceback.print_exc()
        log_utility.log_error(error_msg)
        log_utility.log_error(traceback.format_exc())
        raise Exception(error_msg)


def read_ftp_csv_files(table_path, delimiter, quote_char, host, user_name, password):
    ftp_url = f"ftp://{user_name}:{password}@{host}{table_path}/*.csv"

    try:
        ddf = dd.read_csv(ftp_url, delimiter=delimiter, quote_char=quote_char, dtype='object')
        #validate here it is a concatinated df
        print(ddf.head())
    except (IOError, error_perm) as e:
        print(f"Error reading FTP CSV files: {e}")


def read_sftp_csv_files(table_path, delimiter, quote_char, host, user_name, password, port=22):
    try:
        transport = paramiko.Transport((host, port))
        transport.connect(username=user_name, password=password)
        sftp = paramiko.SFTPClient.from_transport(transport)

        file_list = sftp.listdir(table_path)
        csv_files_sftp = [file for file in file_list if file.lower().endswith('.csv')]

        transport.close()

        ddf = dd.read_csv([f"sftp://{user_name}:{password}@{host}:{port}{table_path}/{csv_file}"
                           for csv_file in csv_files_sftp],
                          delimiter=delimiter, quote_char=quote_char, dtype='object')
        #validate here it is a concatinated df
        print(ddf.head())
        return ddf

    except (paramiko.SSHException, FileNotFoundError) as e:
        print(f"Error reading SFTP CSV files: {e}")
