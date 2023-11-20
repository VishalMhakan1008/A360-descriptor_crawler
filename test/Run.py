import jsonpickle
from flask import Flask, request, jsonify

from bean.MetaColumnInfoBean import MetaColumnInfoBean
from bean.TableBean import TableBean
from processor.FileProcessor import FileProcessor
from processor.MetaDataProcessor import MetadataProcessor

app = Flask(__name__)

@app.route('/process_table', methods=['POST'])
def process_table():
    connection_dto = request.get_json()
    metaData_file_path = connection_dto['connectionType']
    metadata_list = []

    file_processor = FileProcessor()
    flat_file_metaData_list = file_processor.process_file(connection_dto)

    filtered_meta = filter_metadata(metadata_list,flat_file_metaData_list)
    json_list = [jsonpickle.encode(metadata.to_dict()) for metadata in flat_file_metaData_list]

    return jsonify(json_list)


def filter_metadata(metadata_list, flat_file_metadata_list):
    filtered_metadata = []

    for metadata in metadata_list:
        for flat_file_metadata in flat_file_metadata_list:
            if (
                metadata.schema_name == flat_file_metadata.schema_name
                and metadata.table_name == flat_file_metadata.table_name
                and all(
                    col.column_name in [flat_col.column_name for flat_col in flat_file_metadata.columns]
                    for col in metadata.columns
                )
            ):
                filtered_metadata.append(metadata)

    return filtered_metadata



if __name__ == '__main__':
    app.run(debug=True)

