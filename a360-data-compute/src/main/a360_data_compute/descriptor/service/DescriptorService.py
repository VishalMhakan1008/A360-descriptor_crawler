from src.main.a360_data_compute.descriptor.bean.dto.request.ColumnDTO import ColumnDTO
from src.main.a360_data_compute.descriptor.bean.dto.request.ConnectionDTO import ConnectionDTO
from src.main.a360_data_compute.descriptor.bean.dto.request.DescriptorRequestDTO import DescriptorRequestDTO
from src.main.a360_data_compute.descriptor.bean.dto.request.TableRequestDTO import TableRequestDTO
from src.main.a360_data_compute.descriptor.service.Processor import Processor
from src.main.a360_data_compute.utils.CommonConnectionUtils import CommonConnectionUtils
from src.main.a360_data_compute.utils.FileReaderUtils import FileReaderUtils
from src.main.a360_data_compute.utils.LogUtility import LogUtility
import json
import time


class DescriptorService:
    log_utility = LogUtility()

    @staticmethod
    def get_request_dto_from_json(json_data):
        descriptor_dto = json_data.get('requestDTO', {})
        connection_dto = descriptor_dto.get('connectionDTO', {})
        schema_name = descriptor_dto.get('schemaName')
        table_bean = descriptor_dto.get('tableBean', {})
        columns_data = table_bean.get('columns', [])
        columns = [ColumnDTO(name=column.get('name', ''), index=column.get('index', '')) for column in columns_data if column.get('name', '')]
        table_dto = TableRequestDTO(name=table_bean.get('name', ''), columns=columns)
        file_path = connection_dto.get('filePath')
        connection_type = connection_dto.get('connectionType')
        host = connection_dto.get('host', '')
        port = connection_dto.get('port', 0)
        username = connection_dto.get('username', '')
        password = connection_dto.get('password', '')
        delimiter = connection_dto.get('delimiter', '')
        file_format = connection_dto.get('fileFormat', '')

        connection_dto_instance = ConnectionDTO(
            file_path=file_path,
            file_format=file_format,
            delimiter=delimiter,
            connection_type=connection_type,
            metadata_file_path=connection_dto.get('metadataFilePath', ''),
            user_name=username,
            password=password,
            host=host,
            port=port,
            ignore_quotations=connection_dto.get('ignoreQuotations', False),
            with_strict_quotes=connection_dto.get('withStrictQuotes', False),
            is_meta_info_available=connection_dto.get('isMetaInfoAvailable', False),
            quote_character=connection_dto.get('quoteCharacter', '')
        )

        return DescriptorRequestDTO(
            connection_dto=connection_dto_instance,
            table_bean=table_dto,
            columns=columns,
            schema_name=schema_name
        )

    @staticmethod
    def start_process(request_dto, process_id, processes):
        csv_files = []
        sftp = None
        ftp = None
        connection_dto = request_dto.connection_dto
        process = processes[process_id]
        process.status = 'IN_PROGRESS'
        if connection_dto.connection_type == 'SFTP':
            sftp = CommonConnectionUtils.processSFTP(connection_dto.username, connection_dto.port, connection_dto.password)
            csv_files = FileReaderUtils.get_remote_csv_files(sftp, connection_dto.file_path)

        elif connection_dto.connection_type == 'FTP':
            ftp = CommonConnectionUtils.processFTP(connection_dto.password, connection_dto.port, connection_dto.username)
            csv_files = FileReaderUtils.get_remote_csv_files_for_ftp(ftp, connection_dto.file_path)

        elif connection_dto.connection_type == 'Local Storage':
            csv_files = FileReaderUtils.get_local_csv_files(connection_dto.file_path)

        execute = Processor(connection_dto.file_path, process_id)
        combined_metadata = execute.execute_process(csv_files, request_dto, ftp)
        json_str = json.dumps(combined_metadata, indent=2)

        if sftp is not None:
            sftp.close()

        if ftp is not None:
            ftp.quit()

        output_path = execute.end_process(json_str)
        if processes:
            process = processes[process_id]
            process.end_time = time.time()
            process.status = 'COMPLETED'
            process.result_path = output_path
            processes[process_id] = process
            process.update_status()
            DescriptorService.log_utility.log_info(f"Process status: {process.status} for process_id:{process_id}")
        else:
            DescriptorService.log_utility.log_error(f"Process with ID {process_id} not found.")

        return json_str
