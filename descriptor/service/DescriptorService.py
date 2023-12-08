from descriptor.service.Processor import Processor
from utils.CommonConnectionUtils import CommonConnectionUtils
from utils.FileReaderUtils import FileReaderUtils
from utils.LogUtility import LogUtility
import json
import time


class DescriptorService:
    log_utility = LogUtility()

    @staticmethod
    def get_request_dto_from_json(json_data):
        descriptor_dto = json_data.get('descriptorRequestDTO', {})
        connection_dto = descriptor_dto.get('connectionDTO', {})
        tableBean = descriptor_dto.get('tableBean', {})
        file_path = connection_dto.get('filePath')
        file_path = file_path + '/' + tableBean.get('name')
        connection_type = connection_dto.get('connectionType')
        host = connection_dto.get('host', '')
        port = connection_dto.get('port', 0)
        username = connection_dto.get('username', '')
        password = connection_dto.get('password', '')
        delimiter = connection_dto.get('delimiter', '')
        file_format = connection_dto.get('fileFormat', '')

        return {
            'file_path': file_path,
            'connection_type': connection_type,
            'host': host,
            'port': port,
            'username': username,
            'password': password,
            'delimiter': delimiter,
            'file_format': file_format
        }

    @staticmethod
    def start_process(request_dto, process_id, processes):
        csv_files = []
        sftp = None
        ftp = None
        process = processes[process_id]
        process.status = 'IN_PROGRESS'
        if request_dto.get('connection_type') == 'SFTP':
            sftp = CommonConnectionUtils.processSFTP(request_dto.username, request_dto.port, request_dto.password)
            csv_files = FileReaderUtils.get_remote_csv_files(sftp, request_dto.file_path)

        elif request_dto.get('connection_type') == 'FTP':
            ftp = CommonConnectionUtils.processFTP(request_dto.password, request_dto.port, request_dto.username)
            csv_files = FileReaderUtils.get_remote_csv_files_for_ftp(ftp, request_dto.file_path)

        elif request_dto.get('connection_type') == 'Local Storage':
            csv_files = FileReaderUtils.get_local_csv_files(request_dto.get('file_path'))

        execute = Processor(request_dto.get('file_path'), process_id)
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
