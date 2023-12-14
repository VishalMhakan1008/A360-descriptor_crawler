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

        return dd.read_csv(csv_files, delimiter=delimiter, quotechar=quote_char,
                               dtype='object')

    except Exception as e:
        error_msg = f"An unexpected error occurred: {str(e)}"
        print(error_msg)
        traceback.print_exc()
        log_utility.log_error(error_msg)
        log_utility.log_error(traceback.format_exc())
        raise Exception(error_msg)
