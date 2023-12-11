import os
import traceback
from dask.dataframe import from_pandas
import dask.dataframe as dd
from src.main.a360_data_compute.crawler.bean.EnumClass import AccuracyLevel, PropertiesBean, ApprovalStatus
from src.main.a360_data_compute.crawler.bean.RequestDTO import CurrentWorkingCombinationFF, CrawlFlatfileRequestDTO
from src.main.a360_data_compute.crawler.status_monitoring.staus_monitoring import Process_monitoring
from src.main.a360_data_compute.utils.LogUtility import LogUtility

log_utility = LogUtility()


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


# def getting_matching_result(column1, column2):
#     # checking
#     try:
#         matching = dd.DataFrame.isin(column1, column2)
#         return matching.compute()
#     except Exception as e:
#         print(f"Dask Error: {e}")
#         return None


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


# def create_chunk(dataframe, table_size):
#     target_chunk_size_mb = 50
#     try:
#         num_partitions = (table_size // (target_chunk_size_mb * 1024 * 1024)) + 1
#         chunked_dataframe = dataframe.repartition(npartitions=num_partitions)
#
#         # Extract each chunk
#         chunks = []
#         for partition_id in range(num_partitions):
#             # Use get_partition to extract each partition
#             chunk = chunked_dataframe.get_partition(partition_id).compute()
#             chunks.append(chunk)
#
#         return chunks, num_partitions
#
#     except Exception as e:
#         log_utility.log_error(f"Error during repartitioning: {e}")
#         return None


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


def chunk_and_process(first_df, second_df, table_size=1000000000):
    target_chunk_size_mb = 5
    try:
        non_null_df1 = first_df.dropna().drop_duplicates()
        non_null_df2 = second_df.dropna().drop_duplicates()
        var = non_null_df2.npartitions
        print(var)

        num_partitions_df1 = (table_size // (target_chunk_size_mb * 1024 * 1024)) + 1
        num_partitions_df2 = (table_size // (target_chunk_size_mb * 1024 * 1024)) + 1
        # matching data means  delete that 100 %  and matching row also removed
        df1 = non_null_df1.repartition(npartitions=16)
        df2 = non_null_df2.repartition(npartitions=16)

        partitions_df1 = [df1.get_partition(partition_id) for partition_id in range(16)]
        partitions_df2 = [df2.get_partition(partition_id) for partition_id in range(16)]

        matching_values = []

        for partition_df1 in partitions_df1:
            for partition_df2 in partitions_df2:
                column_name_df1 = partition_df1.columns[0]
                column_name_df2 = partition_df2.columns[0]
                try:
                    merged_df = dd.merge(partition_df1, partition_df2, how='inner', left_on=column_name_df1,
                                         right_on=column_name_df2, )
                    matching_count = merged_df.shape[0]
                    matching_values.append(matching_count)
                except Exception as e:
                    print(e)
        total_matching_count = sum(matching_values)
        print("Total Matching Count:", total_matching_count.compute())
    except Exception as e:
        print(f"Error: {e}")


def execute_combinations(list_of_combination_final_set, temp_object: dict, crawl_flatfile_DTO: CrawlFlatfileRequestDTO):
    list_combinations_result = []
    for combination_set in list_of_combination_final_set:
        try:
            dto: CurrentWorkingCombinationFF = combination_set['comb_dto']
            first_df = combination_set['first_df']
            second_df = combination_set['second_df']
            chunk_and_process(first_df, second_df)

        except Exception as e:
            log_utility.log_error(str(e))
            log_utility.log_error(traceback.format_exc())

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


def get_combination_result(process_id):
    try:
        value = final_result[process_id]
        return {
            'processId': process_id,
            'flatFileMatchingResultResponseDTOS': value
        }
    except KeyError:
        raise KeyError("invalid key")
