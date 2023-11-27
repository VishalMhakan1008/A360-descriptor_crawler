import ftplib
import io
import os
import stat

import duckdb
import dask.dataframe as dd
import pandas as pd
import paramiko
from dask import delayed
from dask.distributed import Client

from bean.ColumnBean import ColumnBean
from bean.TableBean import TableBean


class FileProcessor:
    def process_file(self, connection_dto):
        client = Client()
        file_path = connection_dto.get('file_path', None)
        tableName = connection_dto.get('table_name', None)
        #schemaName = connection_dto['schemaName']
        sftp = None
        ftp = None
        local_files = []
        if connection_dto.get('connection_type') == 'SFTP':
            sftp = self.establish_sftp_connection(connection_dto)
            remote_files = self.get_remote_csv_files(sftp, file_path)
            local_files = None
        elif connection_dto.get('connection_type') == 'FTP':
            ftp = self.establish_ftp_connection(connection_dto)
            remote_files = self.get_remote_csv_files_for_ftp(ftp, file_path)
        elif connection_dto.get('connection_type') == 'LocalStorage':
            local_files = self.get_local_csv_files(file_path)
            remote_files = None
        else:
            raise ValueError("Invalid connection type")

        metadata_list = []
        if remote_files:
            for remote_file in remote_files:
                chunk_size = 10
                try:
                    dask_chunks = dd.read_csv(remote_file, blocksize=chunk_size, assume_missing=True)

                    processed_chunks = []
                    futures = []
                    for i, chunk in enumerate(dask_chunks.to_delayed()):
                        future = delayed(self.process_chunk)(chunk, tableName, "schemaName")
                        futures.append(future)

                    try:
                        processed_chunks = client.compute(futures, sync=True)
                    except ValueError as e:
                        print(f"Error processing file: {remote_file}. {e}")

                    concatenated_result = self.concatenate_tables(processed_chunks)

                    metadata_list.append(concatenated_result)
                finally:
                    pass

        if sftp is not None:
            sftp.close()

        if ftp is not None:
            ftp.quit()

        if local_files:
            for local_file in local_files:
                file_size = os.path.getsize(local_file)
                if file_size > 500 * 1024 * 1024:
                    chunk_size = 500 * 1024 * 1024
                else:
                    chunk_size = file_size

                processed_chunks = []
                futures = []
                try:
                    dask_chunks = dd.read_csv(local_file, blocksize=chunk_size, assume_missing=True)

                    for i, chunk in enumerate(dask_chunks.to_delayed()):
                        future = delayed(self.process_chunk)(chunk, tableName, "schemaName")
                        futures.append(future)

                    processed_chunks = client.gather(client.compute(futures))

                    concatenated_result = self.concatenate_tables(processed_chunks)
                    metadata_list.append(concatenated_result)
                finally:
                    pass

        client.close()
        return metadata_list

    def process_chunk(self, chunk, tableName, schemaName):
        metadata = self.generate_metadata(chunk, schemaName, tableName)
        return metadata

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
        self._walk_ftp(ftp, file_path, csv_files)
        return csv_files

    def _walk_ftp(self, ftp, remote_path, csv_files):
        try:
            files = ftp.nlst(remote_path)
            for file in files:
                file_path = os.path.join(remote_path, file)
                try:
                    ftp.cwd(file_path)
                    self._walk_ftp(ftp, file_path, csv_files)
                except ftplib.error_perm:
                    if file.endswith(".csv"):
                        file_content = io.BytesIO()
                        ftp.retrbinary(f"RETR {file}", file_content.write)
                        file_content.seek(0)
                        csv_files.append(file_content)
        except ftplib.error_perm as e:
            print(f"FTP error: {e}")

    def establish_ftp_connection(self, connection_dto):
        ftp = ftplib.FTP()
        ftp.connect(connection_dto.get('host'), int(connection_dto.get('port')))
        ftp.login(connection_dto.get('username'), connection_dto.get('password'))
        return ftp

    def get_local_csv_files(self, file_path):
        csv_files = []

        for root, dirs, files in os.walk(file_path):
            for file in files:
                if file.endswith(".csv"):
                    local_file_path = os.path.join(root, file)
                    csv_files.append(local_file_path)

        return csv_files

    def establish_sftp_connection(self, connection_dto):
        transport = paramiko.Transport((connection_dto.get('host'), int(connection_dto.get('port'))))
        transport.connect(username=connection_dto.get('username'), password=connection_dto.get('password'))
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
                type_length = csv_data[column].str.len().max()
            except AttributeError:
                is_length_uniform = False
                type_length = 0

            column_beans.append(
                ColumnBean(column, data_type, distinct_row_count, null_row_count, all_numeric, is_all_alphabet,
                           is_primary_key, is_date_column, is_length_uniform, type_length))

        column_count = len(csv_data.columns)
        row_count = len(csv_data)

        table_bean = TableBean(column_count, row_count,
                               {col: col_bean for col, col_bean in zip(csv_data.columns, column_beans)}, schema_name,
                               table_name)

        return table_bean

    def extract_schema_and_table_names(self, file_path):
        parts = file_path.split(os.path.sep)

        db_name = parts[-1]
        schema_name = parts[-3] if len(parts) >= 3 else 'DEFAULT_SCHEMA'
        table_name = parts[-2]

        return schema_name, table_name

    def extract_schema_and_table_names_for_sftp(self, sftp_file):
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

    def concatenate_tables(self, table_beans):
        if not table_beans:
            return None

        table_name = table_beans[0].table_name
        schema_name = table_beans[0].schema_name

        concatenated_columns = {}

        for table_bean in table_beans:
            for col_name, col_bean in table_bean.columns.items():
                if col_name not in concatenated_columns:
                    concatenated_columns[col_name] = []

                concatenated_columns[col_name].append(col_bean)

        for col_name, col_beans in concatenated_columns.items():
            concatenated_values = {
                'data_type': col_beans[0].data_type,  # Assuming all data types are the same
                'distinct_row_count': sum(getattr(bean, 'distinct_row_count', 0) for bean in col_beans),
                'null_row_count': sum(getattr(bean, 'null_row_count', 0) for bean in col_beans),
                'all_numeric': any(getattr(bean, 'all_numeric', False) for bean in col_beans),
                'all_alphabet': any(getattr(bean, 'all_alphabet', False) for bean in col_beans),
                'primary_key': any(getattr(bean, 'primary_key', False) for bean in col_beans),
                'is_date_column': any(getattr(bean, 'is_date_column', False) for bean in col_beans),
                'is_length_uniform': any(getattr(bean, 'is_length_uniform', False) for bean in col_beans),
                'type_length': sum(getattr(bean, 'type_length', 0) for bean in col_beans),
            }

            concatenated_columns[col_name] = ColumnBean(column_name=col_name, **concatenated_values)

        concatenated_table_bean = TableBean(
            column_count=sum(bean.column_count for bean in table_beans),
            row_count=sum(bean.row_count for bean in table_beans),
            columns=concatenated_columns,
            schema_name=schema_name,
            table_name=table_name
        )

        return concatenated_table_bean
