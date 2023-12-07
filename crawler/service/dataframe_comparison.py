import os

import numpy as np
from dask import delayed
import dask.dataframe as dd
from crawler.bean.EnumClass import AccuracyLevel, PropertiesBean, ApprovalStatus
from crawler.bean.RequestDTO import CurrentWorkingCombinationFF, CrawlFlatfileRequestDTO
from crawler.status_monitoring.staus_monitoring import Process_monitoring


def checkAccuracyLevel(reverseMatch, forwardMatch):
    if forwardMatch == 100.0 or reverseMatch == 100.0:
        return AccuracyLevel.HIGH
    elif forwardMatch > 80.0 or reverseMatch > 80.0:
        return AccuracyLevel.MEDIUM
    elif forwardMatch > 40.0 or reverseMatch > 40.0:
        return AccuracyLevel.LOW
    else:
        return AccuracyLevel.NOT_RELATED


def getApprovalStatus(confidenceScore):
    if confidenceScore < PropertiesBean.REJECTION_LIMIT:
        return ApprovalStatus.REJECTED
    elif confidenceScore > PropertiesBean.APPROVAL_LIMIT:
        return ApprovalStatus.APPROVED
    else:
        return ApprovalStatus.PENDING


def getting_matching_result(column1, column2):
    matching = np.isin(column1, column2)
    return matching


def process_matching_result(matching_values, non_null_df1, non_null_df2):
    if len(non_null_df1) > 0:
        forward_matching = (matching_values.sum() / len(non_null_df1)) * 100
    else:
        forward_matching = float(0.0)

    if len(non_null_df2) > 0:
        reverse_matching = (matching_values.sum() / len(non_null_df1)) * 100
    else:
        reverse_matching = float(0.0)

    confidenceScore = (reverse_matching + forward_matching) / 2.0
    accuracyLevel = checkAccuracyLevel(reverse_matching, forward_matching)
    approvalStatus = getApprovalStatus(confidenceScore)
    return ({
        'forward_matching': forward_matching,
        'reverse_matching': reverse_matching,
        'confidenceScore': confidenceScore,
        'accuracyLevel': accuracyLevel,
        'approvalStatus': approvalStatus
    })


def create_chunk(filePath, dataframe):
    target_chunk_size_mb = 100
    file_size = os.path.getsize(filePath)
    num_partitions = (file_size // (target_chunk_size_mb * 1024 * 1024)) + 1
    divisions = [i * (file_size // num_partitions) for i in range(num_partitions + 1)]
    chunk = dataframe.repartition(divisions=divisions)
    return chunk, num_partitions


def save_combination_result(combination_result, dto: CurrentWorkingCombinationFF):
    return {
        'taskId': dto.taskId, 'column1Id': dto.column1Id, 'schema1Id': dto.schema1Id, 'table1': dto.tableName1,
        'column2Id': dto.column2Id, 'schema2Id': dto.schema2Id, 'table2': dto.tableName2,
        'forwardMatch': combination_result['forward_matching'], 'reverseMatch': combination_result['reverse_matching'],
        'confidenceScore': combination_result['confidenceScore'],
        'approvalStatus': combination_result['approvalStatus'],
        'accuracyLevel': combination_result['accuracyLevel']
    }


final_result = dict


def execute_combinations(list_of_combination_final_set, temp_object: dict, crawl_flatfile_DTO: CrawlFlatfileRequestDTO):
    list_combinations_result = []
    for combination_set in list_of_combination_final_set:
        dto = CurrentWorkingCombinationFF(**combination_set['dto'])
        first_df = combination_set['first_df']
        second_df = combination_set['second_df']
        df1_chunk, num_partitions1 = create_chunk(dto.tablePath1, first_df)
        df2_chunk, num_partitions2 = create_chunk(dto.tablePath2, second_df)
        combination_result = process_chunk(df1_chunk, df2_chunk, first_df, num_partitions1, num_partitions2, second_df)
        list_combinations_result.append(save_combination_result(combination_result, dto))
    process_id = crawl_flatfile_DTO.processId
    result = {
        'processId': process_id,
        'flatFileMatchingResultResponseDTOS': list_combinations_result
    }

    final_result = result

    if temp_object:
        if process_id in temp_object:
            process_obj = Process_monitoring(**temp_object[process_id])
            process_obj.status = "SUCCESS"
    else:
        print("temp_object is empty.")


def process_chunk(df1_chunk, df2_chunk, first_df, num_partitions1, num_partitions2, second_df):
    matching_values = []
    for i in range(num_partitions1):
        chunk1 = df1_chunk.get_partition(i)
        for j in range(num_partitions2):
            chunk2 = df2_chunk.get_partition(j)
            matching_values = getting_matching_result(chunk1, chunk2)
    return process_matching_result(matching_values,
                                   first_df.dropna(),
                                   second_df.dropna()
                                   )


def get_combination_result(process_id):
    try:
        value = final_result[process_id]
        return value
    except KeyError:
        raise KeyError("invalid key")

