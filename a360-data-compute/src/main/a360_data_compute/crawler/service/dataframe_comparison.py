import os
import traceback

from src.main.a360_data_compute.crawler.bean.enums import AccuracyLevel, PropertiesBean, ApprovalStatus
from src.main.a360_data_compute.crawler.bean.request_response_dtos import CurrentWorkingCombinationFF, CrawlFlatfileRequestDTO, \
    MatchingDTO

from src.main.a360_data_compute.utils.LogUtility import LogUtility
from src.main.a360_data_compute.utils.data_frame_operations import get_matching_count

log_utility = LogUtility()


# cluster = LocalCluster(n_workers=4, threads_per_worker=2, memory_limit="13GB")
# client = Client(cluster)


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
        error_msg = f"Error in process_matching_result: {str(e)}"
        print(error_msg)
        log_utility.log_error(error_msg)
        log_utility.log_error(traceback.format_exc())


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
    except Exception as e:
        error_msg = f"Error in save_combination_result: {str(e)}"
        print(error_msg)
        log_utility.log_error(error_msg)
        log_utility.log_error(traceback.format_exc())


def execute_combinations(list_of_combination_final_set, temp_object: dict, crawl_flatfile_DTO: CrawlFlatfileRequestDTO):
    list_combinations_result = []
    process_id = crawl_flatfile_DTO.processId

    try:
        for combination_set in list_of_combination_final_set:
            try:
                dto: CurrentWorkingCombinationFF = combination_set['comb_dto']
                first_df = combination_set['first_df']
                second_df = combination_set['second_df']
                print("matching_count computation starts.... ")
                matching_count = get_matching_count(first_df, second_df)
                print("matching_count computation done .....")
                result_dto: MatchingDTO = process_matching_result(matching_count, dto.table1rowCount,
                                                                  dto.table2rowCount)
                list_combinations_result.append(save_combination_result(result_dto, dto))

            except Exception as e:
                error_msg = f"Error processing combination: {str(e)}"
                print(error_msg)
                log_utility.log_error(error_msg)
                log_utility.log_error(traceback.format_exc())

        result = {
            'processId': process_id,
            'flatFileMatchingResultResponseDTOS': list_combinations_result
        }

        if temp_object:
            process_id_to_find = process_id
            if process_id_to_find in temp_object:
                process_obj = temp_object[process_id_to_find]
                process_obj.status = "SUCCESS"

    except Exception as e:
        error_msg = f"Error executing combinations: {str(e)}"
        print(error_msg)
        log_utility.log_error(error_msg)
        log_utility.log_error(traceback.format_exc())

        result = {
            'error': error_msg,
            'processId': process_id
        }

    return result

# def get_combination_result(process_id):
#     try:
#         value = final_result[process_id]
#         return {
#             'processId': process_id,
#             'flatFileMatchingResultResponseDTOS': value
#         }
#     except KeyError:
#         raise KeyError("invalid key")
