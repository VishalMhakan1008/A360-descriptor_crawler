import os
from io import BytesIO

from dask import delayed
from dask import dataframe as dd
from utils import CommonConnectionUtils
from crawler.service.dataframe_comparison import comparing_dataframes


@delayed
def process_flatfile(combination):
    columnName1 = combination['columnName1']
    columnName2 = combination['columnName2']

    combination_concat_df = []

    table_path_list = [combination['tablePath1'], combination['tablePath2']]
    connectionType = combination['connectionType']

    for i, table_path in enumerate(table_path_list):

        dataframe_list = reading_file(connectionType, table_path, combination)
        if i == 0:
            column_name = columnName1
        else:
            column_name = columnName2

        single_column_dfs = [ddf[column_name] for ddf in dataframe_list]
        concat_single_column_df = dd.concat(single_column_dfs)
        combination_concat_df.append(concat_single_column_df)
    return comparing_dataframes(combination_concat_df, combination, columnName1, columnName2)





@delayed
def reading_file(connectionType, table_path, combination):
    if connectionType == 'Local Storage':

        files = [
            file
            for file in os.listdir(table_path)
            if file.endswith('.csv')
        ]

        ddf_list = [
            dd.read_csv(os.path.join(table_path, file), dtype='object')
            for file in files
        ]
        return ddf_list
    elif connectionType == 'sftp':
        # sftp = CommonConnectionUtils.CommonConnectionUtils.processSFTP(host,port,username,password)
        files = [
            file
            for file in sftp.listdir(table_path)
            if file.endswith('.csv')]

        ddf_list = [
            dd.read_csv(BytesIO(sftp.open(os.path.join(table_path, file)).read()), dtype='object')
            for file in files
        ]
        return ddf_list
