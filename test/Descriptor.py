import ftplib
import io
import logging
import random
import time
import os

import json
import paramiko as paramiko
from dask.distributed import Client
from flask import Flask, request, jsonify

from bean.Process import Process
from processor.Executor import Execute

if __name__ == '__main__':
    client = Client()
    processes = {}
    app = Flask(__name__)

    @app.route('/process_table', methods=['POST'])
    def process_table():
        request_dto = get_request_dto_from_json(request.get_json())


        process_id = random.randrange(1, 100)

        fut = client.submit(start_process, request_dto, process_id)

        process = Process(process_id, fut, 'IN_PROGRESS')
        processes[process_id] = process

        return jsonify({'process_id': process_id})

    def get_request_dto_from_json(json_data):
        connection_dto = json_data.get('connectionDTO', {})

        file_path = connection_dto.get('file_path')
        connection_type = connection_dto.get('connection_type')
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

        elif request_dto.get('connection_type') == 'LocalStorage':
            csv_files = get_local_csv_files(request_dto.get('file_path'))

        execute = Execute(request_dto.get('file_path'), process_id)
        combined_metadata = execute.executeProcess(csv_files, request_dto)
        json_str = json.dumps(combined_metadata, indent=2)

        if sftp is not None:
            sftp.close()

        if ftp is not None:
            ftp.quit()

        output_path = execute.endProcess(json_str)
        process = processes[process_id]
        process.end_time = time.time()
        process.status = 'COMPLETED'
        process.result_path = output_path
        processes[process_id] = process
        logging.info(process.to_dict_result())

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
