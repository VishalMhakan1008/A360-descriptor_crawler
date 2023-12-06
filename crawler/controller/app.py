import random

from flask import Flask, request, jsonify, current_app
from crawler.service import processFlatfile
from crawler.status_monitoring.staus_monitoring import Process_monitoring

temp_object = {}

app = Flask(__name__)


@app.route('/crawl-process', methods=['POST'])
def processCrawling():
    try:
        process_id = random.randrange(1, 100)
        process = Process_monitoring(process_id, 'IN_PROGRESS')
        temp_object[process_id] = process
        dtos_data = request.get_json()
        processFlatfile.startCrawling(dtos_data)
        current_app.logger.info("Crawling done!")
    except Exception as e:
        return jsonify({'error': str(e)}), 500

    return jsonify({'process_id': process_id})


@app.route('/process_status/<int:process_id>', methods=['GET'])
def check_status(processId):
    if processId not in temp_object:
        return jsonify({'error': 'Process not found'}), 404
    process = temp_object[processId]

    return jsonify({'status': process.status})


@app.route('/process_details/<int:process_id>', methods=['GET'])
def get_combination_result(processId):
# access the list of combination dto(RETURN LIST OF DTO )


if __name__ == "__main__":
    app.run(debug=True)
