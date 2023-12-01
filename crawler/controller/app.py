from flask import Flask, request, jsonify, current_app

from crawler.service import processFlatfile

app = Flask(__name__)

if __name__ == "__main__":
    app.run(debug=True)


    @app.route('/crawl-process', methods=['POST'])
    def processCrawling():
        try:
            dtos_data = request.get_json()

            processFlatfile.startCrawling(dtos_data)

            current_app.logger.info("Crawling done!")
        except Exception as e:
            current_app.logger.error(f"Error: {str(e)}")
            return jsonify({'error': str(e)}), 500  # Return an error response

        return "Crawling done!"
