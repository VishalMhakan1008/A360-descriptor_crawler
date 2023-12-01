import ftplib

import pandas as pd
import paramiko

from descriptor.bean.MetaColumnInfoBean import MetaColumnInfoBean
from descriptor.bean.MetaTableInfoBean import MetaTableInfoBean


class MetadataProcessor:
    def process_metadata(self, connection_dto):
        metadata_file_path = connection_dto.get('metadataFilePath', None)
        connection_type = connection_dto.get('connection_type', None)

        if connection_type == 'SFTP':
            sftp = self.establish_sftp_connection(connection_dto)
            metadata_csv_data = self.read_csv_from_sftp(sftp, metadata_file_path)
        elif connection_type == 'FTP':
            ftp = self.establish_ftp_connection(connection_dto)
            metadata_csv_data = self.read_csv_from_ftp(ftp, metadata_file_path)
        elif connection_type == 'LocalStorage':
            metadata_csv_data = pd.read_csv(metadata_file_path)
        else:
            raise ValueError("Invalid connection type")

        table_name_column_map = {}
        for index, row in metadata_csv_data.iterrows():
            schema_name = row['SCHEMA_NAME']
            table_name = row['TABLE_NAME']
            column_name = row['COLUMN_NAME']
            data_type = row['DATA_TYPE']
            type_length = row['TYPE_LENGTH']

            column_info = MetaColumnInfoBean(
                column_name=column_name,
                data_type=data_type,
                type_length=type_length
            )

            if schema_name not in table_name_column_map:
                table_name_column_map[schema_name] = {}

            if table_name not in table_name_column_map[schema_name]:
                table_name_column_map[schema_name][table_name] = []

            table_name_column_map[schema_name][table_name].append(column_info)

        table_beans = []
        for schema_name, table_info in table_name_column_map.items():
            for table_name, columns in table_info.items():
                table_beans.append(MetaTableInfoBean(
                    schema_name=schema_name,
                    table_name=table_name,
                    columns=columns
                ))

        return table_beans

    def establish_sftp_connection(self, connection_dto):
        transport = paramiko.Transport((connection_dto.get('host', None), int(connection_dto.get('port', None))))
        transport.connect(username=connection_dto.get('username', None), password=connection_dto.get('password', None))
        sftp = transport.open_sftp_client()
        return sftp

    def read_csv_from_sftp(self, sftp, file_path):
        with sftp.open(file_path, 'r') as file:
            metadata_csv_data = pd.read_csv(file)
        return metadata_csv_data

    def establish_ftp_connection(self, connection_dto):
        ftp = ftplib.FTP()
        ftp.connect(connection_dto.get('host', None), int(connection_dto.get('port', None)))
        ftp.login(connection_dto.get('username', None), connection_dto.get('password', None))
        return ftp

    def read_csv_from_ftp(self, ftp, file_path):
        with ftp.open(file_path, 'r') as file:
            metadata_csv_data = pd.read_csv(file)
        return metadata_csv_data
