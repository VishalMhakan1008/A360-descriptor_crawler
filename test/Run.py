import jsonpickle
from flask import Flask, request, jsonify

from processor.FileProcessor import FileProcessor
from processor.MetaDataProcessor import MetadataProcessor

app = Flask(__name__)

@app.route('/process_table', methods=['POST'])
def process_table():
    request_dto = get_request_dto_from_json(request.get_json())
    metaData_file_path = request_dto.get('metadataFilePath', None)
    metadata_list = []
    filtered_meta = []
    if metaData_file_path:
        metadata_processor = MetadataProcessor()
        metadata_list = metadata_processor.process_metadata(request_dto)


    file_processor = FileProcessor()
    flat_file_metaData_list = file_processor.process_file(request_dto)

    if metadata_list:
        filtered_meta = filter_metadata(metadata_list, flat_file_metaData_list)

    if filtered_meta:
        json_list = [jsonpickle.encode(metadata) for metadata in filtered_meta]
    else:
        json_list = [jsonpickle.encode(metadata.to_dict()) for metadata in flat_file_metaData_list]

    return jsonify(json_list)


def filter_metadata(metadata_list, flat_file_metadata_list):
    filtered_metadata = []

    for metadata in metadata_list:
        matched_data = None
        for flat_file_metadata in flat_file_metadata_list:
            if (
                    metadata.schema_name == flat_file_metadata.schema_name
                    and metadata.table_name == flat_file_metadata.table_name
            ):
                matched_columns = [
                    flat_file_metadata.columns[column_name]
                    for column_name in [col.column_name for col in metadata.columns]
                    if column_name in flat_file_metadata.columns
                ]

                if matched_columns:
                    matched_data = {
                        "schema_name": flat_file_metadata.schema_name,
                        "table_name": flat_file_metadata.table_name,
                        "columns": matched_columns
                    }

        if matched_data:
                filtered_metadata.append(matched_data)

    return filtered_metadata

def get_request_dto_from_json(json_data):
    connection_dto = json_data.get('connectionDTO', {})
    table_bean = json_data.get('tableBean', {})

    file_path = connection_dto.get('file_path', '')
    connection_type = connection_dto.get('connection_type', '')
    host = connection_dto.get('host', '')
    port = connection_dto.get('port', 0)
    username = connection_dto.get('username', '')
    password = connection_dto.get('password', '')
    delimiter = connection_dto.get('delimiter', '')
    file_format = connection_dto.get('file_format', '')
    metadata_file_path = connection_dto.get('metadataFilePath', '')

    table_name = table_bean.get('name', '')
    columns = table_bean.get('columns', [])

    return {
        'file_path': file_path,
        'connection_type': connection_type,
        'host': host,
        'port': port,
        'username': username,
        'password': password,
        'delimiter': delimiter,
        'file_format': file_format,
        'metadataFilePath': metadata_file_path,
        'table_name': table_name,
        'columns': columns
    }

if __name__ == '__main__':
    app.run(debug=True)

