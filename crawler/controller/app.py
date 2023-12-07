import random

from flask import Flask, request, jsonify, current_app

from crawler.bean.RequestDTO import CrawlFlatfileRequestDTO
from crawler.service import processFlatfile
from crawler.status_monitoring.staus_monitoring import Process_monitoring

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
        process_id = dtos_data['process_id']
        process = Process_monitoring(process_id, 'IN_PROGRESS')
        temp_object[process_id] = process
        request_dto = CrawlFlatfileRequestDTO(**dtos_data)
        processFlatfile.startCrawling(request_dto, temp_object)
        current_app.logger.info("Crawling done!")
    except Exception as e:
        return jsonify({'error': str(e)}), 500

    return jsonify({'processStatus': process.status})


@app.route('/process_status/<int:process_id>', methods=['GET'])
def check_status(processId):
    if processId not in temp_object:
        return jsonify({'error': 'Process not found'}), 404
    process = temp_object[processId]

    return jsonify({'status': process.status})


# @app.route('/process_details/<int:process_id>', methods=['GET'])
# def get_combination_result(processId):
#
#
# # access the list of combination dto(RETURN LIST OF DTO )


if __name__ == "__main__":
    app.run(debug=True)
