import ftplib
import os
import stat

import duckdb
import pandas as pd
import paramiko

from bean.ColumnBean import ColumnBean
from bean.TableBean import TableBean


class FileProcessor:
    def process_file(self, connection_dto):
        schema_table_map = {}
        file_path = connection_dto['filePath']
        sftp = None
        ftp = None
        if connection_dto['connectionType'] == 'SFTP':
            sftp = self.establish_sftp_connection(connection_dto)
            remote_files = self.get_remote_csv_files(sftp, file_path)
            local_files = None
        elif connection_dto['connectionType'] == 'FTP':
            ftp = self.establish_ftp_connection(connection_dto)
            remote_files = self.get_remote_csv_files_for_ftp(ftp, file_path)
        elif connection_dto['connectionType'] == 'LocalStorage':
            local_files = self.get_local_csv_files(file_path)
            remote_files = None
        else:
            raise ValueError("Invalid connection type")

        metadata_list = []
        if remote_files:
            for remote_file in remote_files:
                try:
                    csv_data = pd.read_csv(remote_file, encoding='latin1')
                    # schema_name, table_name = self.extract_schema_and_table_names_for_sftp(remote_file)
                    metadata = self.generate_metadata(csv_data, 'schema_name','table_name')
                    metadata_list.append(metadata)
                except UnicodeDecodeError as e:
                    print(f"Error decoding file: {remote_file}. {e}")
        sftp.close()
        ftp.quit()
        if local_files:
            for local_file in local_files:
                csv_data = pd.read_csv(local_file)
                schema_name, table_name = self.extract_schema_and_table_names(local_file)
                metadata = self.generate_metadata(csv_data, schema_name, table_name)
                metadata_list.append(metadata)

        return metadata_list


    def get_remote_csv_files(self, sftp, file_path):
        csv_files = []
        self._walk(sftp, file_path, csv_files)
        return csv_files

    def _walk(self, sftp, remote_path, csv_files):
        for entry in sftp.listdir_attr(remote_path):
            entry_path = os.path.join(remote_path, entry.filename)

            if stat.S_ISDIR(entry.st_mode):
                self._walk(sftp, entry_path, csv_files)
            elif stat.S_ISREG(entry.st_mode) and entry.filename.endswith(".csv"):
                csv_files.append(sftp.open(entry_path))

    def get_remote_csv_files_for_ftp(self, ftp, file_path):
        csv_files = []

        directories = file_path.split('/')

        ftp.cwd('/')

        for directory in directories:
            if directory:
                ftp.cwd(directory)

        files = ftp.nlst()

        for file in files:
            if file.endswith(".csv"):
                csv_files.append(file)

        return csv_files

    def establish_ftp_connection(self, connection_dto):
        ftp = ftplib.FTP()
        ftp.connect(connection_dto['host'], int(connection_dto['port']))
        ftp.login(connection_dto['username'], connection_dto['password'])
        return ftp

    def get_local_csv_files(self, file_path):
        csv_files = []

        for root, dirs, files in os.walk(file_path):
            for file in files:
                if file.endswith(".csv"):
                    local_file_path = os.path.join(root, file)
                    csv_files.append(local_file_path)

        return csv_files

    def establish_sftp_connection(self,connection_dto):
        transport = paramiko.Transport((connection_dto['host'], int(connection_dto['port'])))
        transport.connect(username=connection_dto['username'], password=connection_dto['password'])
        sftp = transport.open_sftp_client()
        return sftp


    def generate_metadata(self, csv_data, schema_name, table_name):
        column_beans = []
        is_all_alphabet: bool

        for column in csv_data.columns:

            distinct_row_count = len(csv_data[column].unique())
            null_row_count = csv_data[column].isnull().sum().item()
            all_numeric = all(pd.to_numeric(csv_data[column], errors='coerce').notna())
            try:
                is_all_alphabet = all(isinstance(s, str) and s.isalpha() for s in csv_data[column].dropna())
            except AttributeError:
                is_all_alphabet = False

            unique_count = len(csv_data[column].unique())
            is_primary_key = unique_count == len(csv_data)

            is_date_column = False

            data_type = str(csv_data[column].dtypes)
            if data_type in ['object', 'string']:
                df = duckdb.from_df(csv_data)
                data_type = str(df[column].dtypes)
                if data_type in ['DATE', 'DATE_TIME', 'TIME']:
                    is_date_column = True
            try:
                is_length_uniform = csv_data[column].str.len().nunique() == 1
            except AttributeError:
                is_length_uniform = False

            column_beans.append(ColumnBean(column, data_type, distinct_row_count, null_row_count, all_numeric, is_all_alphabet, is_primary_key, is_date_column, is_length_uniform,type_length=10))

        column_count = len(csv_data.columns)
        row_count = len(csv_data)

        table_bean = TableBean(column_count, row_count, {col: col_bean for col, col_bean in zip(csv_data.columns, column_beans)}, schema_name, table_name)

        return table_bean

    def extract_schema_and_table_names(self, file_path):
        parts = file_path.split(os.path.sep)

        db_name = parts[-1]
        schema_name = parts[-3] if len(parts) >= 3 else 'DEFAULT_SCHEMA'
        table_name = parts[-2]

        return schema_name, table_name

    def extract_schema_and_table_names_for_sftp(self,sftp_file):
        if hasattr(sftp_file, 'sftp') and hasattr(sftp_file.sftp, 'stat'):
            file_info = sftp_file.sftp.stat(sftp_file)

            file_path = file_info.filename

            parts = file_path.split('/')

            db_name = parts[-2] if len(parts) >= 2 else 'DEFAULT_DB'
            schema_name = parts[-3] if len(parts) >= 3 else 'DEFAULT_SCHEMA'
            table_name = parts[-1]

            return schema_name, table_name
        else:
            return 'DEFAULT_DB', 'DEFAULT_SCHEMA', 'DEFAULT_TABLE'