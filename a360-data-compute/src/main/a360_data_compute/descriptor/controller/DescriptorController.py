import os
import random
import dask
from dask.distributed import wait, Client
from flask import Flask, request, jsonify
from src.main.a360_data_compute.descriptor.bean.Process import Process
from src.main.a360_data_compute.descriptor.service.DescriptorService import DescriptorService
from src.main.a360_data_compute.utils.CommonConnectionUtils import CommonConnectionUtils
from src.main.a360_data_compute.utils.LogUtility import LogUtility

dask.config.set(scheduler='debug')

debug = eval(os.environ.get("DEBUG", "False"))

if __name__ == '__main__':
    client = Client()
    app = Flask(__name__)

    log_utility = LogUtility()

    @app.route('/status', methods=['GET'])
    def status_table():
        json_data = request.get_json()
        process_id = json_data.get('process_id', None)
        if process_id is None:
            log_utility.log_warning("Invalid request for status with no process_id")
            return jsonify({'error': 'Invalid request'})

        host = 'localhost'
        port = '5434'
        database = 'postgres'
        user = 'postgres'
        password = 'postgres'
        connection = CommonConnectionUtils.get_postgres_connection(host, port, database, user, password)
        if connection:
            try:
                status = CommonConnectionUtils.get_process_status(connection, process_id)
                return status
            finally:
                CommonConnectionUtils.close_connection(connection)
        else:
            log_utility.log_error("Failed to establish PostgreSQL connection.")
            return jsonify({'error': 'Failed to connect to the database'})

    @app.route('/process_table', methods=['POST'])
    def process_table():
        request_dto = DescriptorService.get_request_dto_from_json(request.get_json())
        processes = {}
        process_id = random.randrange(1000, 1000000)
        process = Process(process_id, None, 'SCHEDULED')
        processes[process_id] = process

        fut = client.submit(lambda: DescriptorService.start_process(request_dto, process_id, processes))

        process.future = fut
        process.status = 'IN_PROGRESS'
        process.create_record()
        wait([fut])
        result = fut.result()
        status = fut.status

        log_utility.log_info(f"Task status: {status}")

        if status == 'error':
            exception_info = client.get_task_exception(fut.key)
            log_utility.log_error(f"Exception info: {exception_info}")

        return jsonify({'process_id': process_id, 'result': result})

    app.run(debug=True, use_reloader=False)
