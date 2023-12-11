import random

from flask import Flask, request, jsonify, current_app
from src.main.a360_data_compute.crawler.bean.RequestDTO import CrawlFlatfileRequestDTO
from src.main.a360_data_compute.crawler.service import processFlatfile, dataframe_comparison
from src.main.a360_data_compute.crawler.status_monitoring.staus_monitoring import Process_monitoring

app = Flask(__name__)
temp_object = {}


@app.route('/generate-proces_id', methods=['GET'])
def generate_processId():
    process_id = random.randrange(1000, 1000000)
    process = Process_monitoring(process_id, 'NOT_STARTED')
    temp_object[process_id] = process
    return jsonify({'processId': process_id})


@app.route('/crawl-process', methods=['POST'])
def processCrawling():
    try:
        dtos_data = request.get_json()
        process_id = int(dtos_data['processId'])
        process = temp_object[process_id]
        process.status = "IN_PROGRESS"
        request_dto = CrawlFlatfileRequestDTO(**dtos_data)
        processFlatfile.startCrawling(request_dto, temp_object)
        current_app.logger.info("Crawling done!")
    except Exception as e:
        return jsonify({'error': str(e)}), 500

    return jsonify({'status': process.status})


@app.route('/process_status/<int:processId>', methods=['GET'])
def check_status(processId):
    if processId not in temp_object:
        return jsonify({'error': 'Process not found'}), 404
    process = temp_object[processId]

    return jsonify({'status': process.status})


@app.route('/process_details/<int:processId>', methods=['GET'])
def get_combination_result(processId):
    result = dataframe_comparison.get_combination_result(processId)
    return jsonify(result)


if __name__ == "__main__":
    app.run(debug=True)
