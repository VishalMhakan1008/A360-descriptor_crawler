import os
import traceback

import dask
from dask.dataframe import from_pandas
import dask.dataframe as dd
from flask import jsonify

from src.main.a360_data_compute.crawler.bean.EnumClass import AccuracyLevel, PropertiesBean, ApprovalStatus
from src.main.a360_data_compute.crawler.bean.RequestDTO import CurrentWorkingCombinationFF, CrawlFlatfileRequestDTO, \
    MatchingDTO
from src.main.a360_data_compute.crawler.status_monitoring.staus_monitoring import Process_monitoring
# from src.main.a360_data_compute.crawler.status_monitoring.staus_monitoring import Process_monitoring
from src.main.a360_data_compute.utils.LogUtility import LogUtility

log_utility = LogUtility()


def checkAccuracyLevel(reverseMatch, forwardMatch):
    if forwardMatch == 100.0 or reverseMatch == 100.0:
        return AccuracyLevel.HIGH.value
    elif forwardMatch > 80.0 or reverseMatch > 80.0:
        return AccuracyLevel.MEDIUM.value
    elif forwardMatch > 40.0 or reverseMatch > 40.0:
        return AccuracyLevel.LOW.value
    else:
        return AccuracyLevel.NOT_RELATED.value


def getApprovalStatus(confidenceScore):
    if confidenceScore < PropertiesBean.REJECTION_LIMIT.value:
        return ApprovalStatus.REJECTED.value
    elif confidenceScore > PropertiesBean.APPROVAL_LIMIT.value:
        return ApprovalStatus.APPROVED.value
    else:
        return ApprovalStatus.PENDING.value


def process_matching_result(matching_values, table1rowCount, table2rowCount):
    try:
        if table1rowCount > 0:
            forward_matching_count = (matching_values / table1rowCount) * 100
            forward_matching = round(forward_matching_count, 3)
        else:
            forward_matching = float(0.0)

        if table2rowCount > 0:
            reverse_matching_count = (matching_values / table2rowCount) * 100
            reverse_matching = round(reverse_matching_count, 3)


        else:
            reverse_matching = float(0.0)

        confidenceScore = (reverse_matching + forward_matching) / 2.0
        accuracyLevel = checkAccuracyLevel(reverse_matching, forward_matching)
        approvalStatus = getApprovalStatus(confidenceScore)
        dto = MatchingDTO(forward_matching, reverse_matching, accuracyLevel, approvalStatus, confidenceScore)
        return dto
    except Exception as e:
        print(e)


def save_combination_result(match_dto: MatchingDTO, dto: CurrentWorkingCombinationFF):
    try:
        result = {
            'taskId': dto.taskId, 'column1Id': dto.column1Id, 'schema1Id': dto.schema1Id, 'table1': dto.tableName1,
            'column2Id': dto.column2Id, 'schema2Id': dto.schema2Id, 'table2': dto.tableName2,
            'forwardMatch': match_dto.forward_matching, 'reverseMatch': match_dto.reverse_matching,
            'confidenceScore': match_dto.confidence_score, 'approvalStatus': match_dto.approval_status,
            'accuracyLevel': match_dto.accuracy_level
        }
        return result
    except Exception as ee:
        print(ee)


final_result = dict


def chunk_and_process(first_df, second_df):
    try:
        non_null_df1 = first_df.drop_duplicates()
        non_null_df2 = second_df.drop_duplicates()
        df1 = non_null_df1.repartition(npartitions=first_df.npartitions)
        df2 = non_null_df2.repartition(npartitions=second_df.npartitions)
        matching_values = []

        for partition_df_1_id in range(first_df.npartitions):
            temp_df_1 = df1.get_partition(partition_df_1_id)
            for partition_df_2_id in range(second_df.npartitions):
                temp_df_2 = df2.get_partition(partition_df_2_id)
                column_name_df1 = temp_df_1.columns[0]
                column_name_df2 = temp_df_2.columns[0]
                try:
                    merged_df = dd.merge(temp_df_1, temp_df_2, how='inner', left_on=column_name_df1,
                                         right_on=column_name_df2, )
                    # matching_count = temp_df_1[column_name_df1].isin(temp_df_2[column_name_df2]).sum()

                    matching_count = merged_df.shape[0]
                    matching_values.append(matching_count)
                except Exception as e:
                    print(e)
        total_matching_count = dask.compute(sum(matching_values))[0]
        print("Total Matching Count:", total_matching_count)
        return total_matching_count
    except Exception as e:
        print(f"Error: {e}")


def execute_combinations(list_of_combination_final_set, temp_object: dict, crawl_flatfile_DTO: CrawlFlatfileRequestDTO):
    list_combinations_result = []
    for combination_set in list_of_combination_final_set:
        try:
            dto: CurrentWorkingCombinationFF = combination_set['comb_dto']
            first_df = combination_set['first_df']
            second_df = combination_set['second_df']
            matching_count = chunk_and_process(first_df, second_df)
            result_dto: MatchingDTO = process_matching_result(matching_count, dto.table1rowCount, dto.table2rowCount)
            list_combinations_result.append(save_combination_result(result_dto, dto))

        except Exception as e:
            print(e, "process combination error ")
            log_utility.log_error(str(e))
            log_utility.log_error(traceback.format_exc())

    process_id = crawl_flatfile_DTO.processId
    result = {
        'processId': process_id,
        'flatFileMatchingResultResponseDTOS': list_combinations_result
    }
    print(result)

    final_result = result

    if temp_object:
        process_id_to_find = process_id

        if process_id_to_find in temp_object:
            process_obj = temp_object[process_id_to_find]
            process_obj.status = "SUCCESS"


def get_combination_result(process_id):
    try:
        value = final_result[process_id]
        return {
            'processId': process_id,
            'flatFileMatchingResultResponseDTOS': value
        }
    except KeyError:
        raise KeyError("invalid key")
