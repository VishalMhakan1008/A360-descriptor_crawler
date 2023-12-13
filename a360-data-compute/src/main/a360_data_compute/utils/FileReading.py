import os
import traceback
from dask import dataframe as dd
import os
import traceback
import pandas as pd
import dask.dataframe as dd
from paramiko import SSHClient, AutoAddPolicy
from ftplib import FTP
from io import BytesIO

from src.main.a360_data_compute.crawler.service.dataframe_comparison import log_utility


def read_local_csv(table_path, delimiter, quote_char, columnName=None, header=None):
    try:
        if not table_path.lower().endswith('.csv'):
            global csv_files
            if os.path.exists(table_path):
                csv_files = [
                    os.path.join(table_path, file)
                    for file in os.listdir(table_path)
                    if file.endswith('.csv')
                ]
            else:
                raise FileNotFoundError(f"Files do not exist in the path:", table_path)

        elif table_path.lower().endswith('.csv'):
            csv_files = table_path

        else:
            raise ValueError("Unsupported file format. Supported formats are CSV.")

        if columnName:
            return dd.read_csv(csv_files, delimiter=delimiter, quotechar=quote_char,
                               header=1, names=[columnName], dtype='object')
        else:
            return dd.read_csv(csv_files, delimiter=delimiter, quotechar=quote_char,
                               header=1, dtype='object')

    except Exception as e:
        error_msg = f"An unexpected error occurred: {str(e)}"
        print(error_msg)
        traceback.print_exc()
        log_utility.log_error(error_msg)
        log_utility.log_error(traceback.format_exc())
        raise Exception(error_msg)


def read_file_from_sftp(host, port, username, password, remote_path, local_path, delimiter, quote_char,
                        columnName=None, header=None):
    try:
        ssh = SSHClient()
        ssh.set_missing_host_key_policy(AutoAddPolicy())
        ssh.connect(host, port, username, password)

        sftp = ssh.open_sftp()
        remote_file = sftp.open(remote_path, 'r')
        file_content = remote_file.read()
        remote_file.close()

        local_file_path = os.path.join(local_path, os.path.basename(remote_path))
        with open(local_file_path, 'wb') as local_file:
            local_file.write(file_content)

        return read_local_csv(local_file_path, delimiter, quote_char, columnName, header)
    except Exception as e:
        handle_exception(e)


def read_file_from_ftp(host, port, username, password, remote_path, local_path, delimiter, quote_char,
                       columnName=None, header=None):
    try:
        ftp = FTP()
        ftp.connect(host, port)
        ftp.login(username, password)

        local_file_path = os.path.join(local_path, os.path.basename(remote_path))
        with open(local_file_path, 'wb') as local_file:
            ftp.retrbinary(f"RETR {remote_path}", local_file.write)

        return read_local_csv(local_file_path, delimiter, quote_char, columnName, header)
    except Exception as e:
        handle_exception(e)


def handle_exception(exception):
    error_msg = f"An unexpected error occurred: {str(exception)}"
    print(error_msg)
    traceback.print_exc()
    log_utility.log_error(error_msg)
    log_utility.log_error(traceback.format_exc())
    raise Exception(error_msg)
