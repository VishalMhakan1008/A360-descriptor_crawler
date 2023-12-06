import os
from io import BytesIO

from dask import delayed
from dask import dataframe as dd
from utils import CommonConnectionUtils as cu
from crawler.service.dataframe_comparison import comparing_dataframes


@delayed
def process_flatFile(combination):
    columnName1 = combination['columnName1']
    columnName2 = combination['columnName2']

    combination_concat_df = []

    table_path_list = [combination['tablePath1'], combination['tablePath2']]
    connectionType = combination['connectionType']

    for i, table_path in enumerate(table_path_list):
        if i == 0:
            column_name = columnName1
        else:
            column_name = columnName2
        single_column_df = reading_file(connectionType, table_path, column_name)
        combination_concat_df.append(single_column_df)
    return comparing_dataframes(combination_concat_df, combination, columnName1, columnName2)


@delayed
def reading_file(connectionType, table_path, combination, columnName):
    if connectionType == 'Local Storage':

        csv_files = [
            os.path.join(table_path, file)
            for file in os.listdir(table_path)
            if file.endswith('.csv')
        ]
        single_column_df = dd.read_csv(csv_files,
                                       names=[columnName],
                                       skiprows=1,
                                       dtype='object')

        return single_column_df
    elif connectionType == 'sftp':
        sftp = cu.CommonConnectionUtils.processSFTP(combination['host'], combination['username'],
                                                    combination['port'], combination['password'])
        files = [
            file
            for file in sftp.listdir(table_path)
            if file.endswith('.csv')
        ]

        ddf_list = [
            dd.read_csv(BytesIO(sftp.open(os.path.join(table_path, file)).read()), names=[columnName], dtype='object')
            for file in files
        ]

        concatenated_df = dd.concat(ddf_list)

        return concatenated_df
