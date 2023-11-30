import ftplib
import io
import logging
import random
import time
import os

import json
from dask.distributed import wait
import paramiko as paramiko
from dask.distributed import Client
from flask import Flask, request, jsonify

from bean.Process import Process
from processor.Executor import Execute
debug = eval(os.environ.get("DEBUG", "False"))
processes = {}

if __name__ == '__main__':
    client = Client()
    app = Flask(__name__)

    @app.route('/process_table', methods=['POST'])
    def process_table():
        request_dto = get_request_dto_from_json(request.get_json())


        process_id = random.randrange(1000, 1000000)

        fut = client.submit(start_process, request_dto, process_id)
        process = Process(process_id, fut, 'IN_PROGRESS')
        process.create_record()
        processes[process_id] = process
        wait([fut])

        result = fut.result()
        status = fut.status
        print(f"Task status: {status}")

        if status == 'error':
            exception_info = client.get_task_exception(fut.key)
            print(f"Exception info: {exception_info}")

        return jsonify({'process_id': process_id, 'result': result})

    def get_request_dto_from_json(json_data):
        connection_dto = json_data.get('connectionDTO', {})
        tableBean = json_data.get('tableBean', {})
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

    def start_process(request_dto, process_id):
        csv_files = []
        sftp = None
        ftp = None
        if request_dto.get('connection_type') == 'SFTP':
            sftp = processSFTP(request_dto.host, request_dto.username, request_dto.port, request_dto.password)
            csv_files = get_remote_csv_files(sftp, request_dto.file_path)

        elif request_dto.get('connection_type') == 'FTP':
            ftp = processFTP(request_dto.host, request_dto.password, request_dto.port, request_dto.username)
            csv_files = get_remote_csv_files_for_ftp(ftp, request_dto.file_path)

        elif request_dto.get('connection_type') == 'Local Storage':
            csv_files = get_local_csv_files(request_dto.get('file_path'))

        execute = Execute(request_dto.get('file_path'), process_id)
        combined_metadata = execute.executeProcess(csv_files, request_dto)
        json_str = json.dumps(combined_metadata, indent=2)

        if sftp is not None:
            sftp.close()

        if ftp is not None:
            ftp.quit()

        output_path = execute.endProcess(json_str)
        if process_id in processes:
            process = processes[process_id]
            process.end_time = time.time()
            process.status = 'COMPLETED'
            process.result_path = output_path
            processes[process_id] = process
            process.update_status()
            logging.info(process.to_dict_result())
        else:
            print(f"Process with ID {process_id} not found.")

        return json_str

    def processSFTP(host, username, port, password):
        transport = paramiko.Transport(host, port)
        transport.connect(username, password)
        sftp = transport.open_sftp_client()
        return sftp

    def processFTP(host, password, port, username):
        ftp = ftplib.FTP()
        ftp.connect(host, port)
        ftp.login(username, password)
        return ftp

    def get_remote_csv_files_for_ftp(ftp, file_path):
        try:
            file_content = io.BytesIO()
            ftp.retrbinary(f"RETR {file_path}", file_content.write)
            file_content.seek(0)
            return [file_content]
        except ftplib.error_perm as e:
            print(f"FTP error: {e}")
            return []

    def get_remote_csv_files(sftp, file_path):
        try:
            # Attempt to open the CSV file
            csv_file = sftp.open(file_path)
            return [csv_file]
        except FileNotFoundError:
            print(f"File not found: {file_path}")
            return []
        except Exception as e:
            print(f"Error opening file: {e}")
            return []

    def get_local_csv_files(file_path):
        csv_files = []

        for root, dirs, files in os.walk(file_path):
            for file in files:
                if file.endswith(".csv"):
                    local_file_path = os.path.join(root, file)
                    csv_files.append(local_file_path)

        return csv_files

    app.run(debug=True, use_reloader=False)
