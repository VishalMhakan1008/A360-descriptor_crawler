import os
import dask.delayed
from src.main.a360_data_compute.utils import CommonConnectionUtils as cu


@dask.delayed
def start_validation(combinations):
    validated_combination = []

    for combination in combinations:
        approved_path_count = 0
        table_path_list = [combination['tablePath1'], combination['tablePath2']]
        connectionType = combination['connectionType']
        if connectionType == 'Local Storage':
            for table_path in table_path_list:
                if os.path.exists(table_path) and os.path.isdir(table_path):
                    files = os.listdir(table_path)
                    if files:
                        approved_path_count += 1
            if approved_path_count == 2:
                validated_combination.append(combination)

        elif connectionType == 'sftp':
            try:
                sftp = cu.CommonConnectionUtils.processSFTP(combination['host'], combination['username'],
                                                            combination['port'], combination['password'])
                for table_path in table_path_list:
                    try:
                        sftp.stat(table_path)
                        files = sftp.listdir(table_path)
                        if files:
                            approved_path_count += 1
                    except Exception as e:
                        print(f"Error listing files in {table_path}: {e}")

                if approved_path_count == 2:
                    validated_combination.append(combination)
            except Exception as e:
                print(f"Error establishing SFTP connection: {e}")

    return validated_combination
