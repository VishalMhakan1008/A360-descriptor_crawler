import os

import numpy as np
from dask import delayed
import dask.dataframe as dd
from crawler.bean.EnumClass import AccuracyLevel, PropertiesBean, ApprovalStatus


@delayed
def checkAccuracyLevel(reverseMatch, forwardMatch):
    if forwardMatch == 100.0 or reverseMatch == 100.0:
        return AccuracyLevel.HIGH
    elif forwardMatch > 80.0 or reverseMatch > 80.0:
        return AccuracyLevel.MEDIUM
    elif forwardMatch > 40.0 or reverseMatch > 40.0:
        return AccuracyLevel.LOW
    else:
        return AccuracyLevel.NOT_RELATED


@delayed
def getApprovalStatus(confidenceScore):
    if confidenceScore < PropertiesBean.REJECTION_LIMIT:
        return ApprovalStatus.REJECTED
    elif confidenceScore > PropertiesBean.APPROVAL_LIMIT:
        return ApprovalStatus.APPROVED
    else:
        return ApprovalStatus.PENDING


@delayed
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

    return forward_matching, reverse_matching, confidenceScore, accuracyLevel, approvalStatus


@delayed
def comparingColumns(column1, column2, combination):
    df1 = column1.dropna()
    df2 = column2.dropna()
    df1_chunk, num_partitions1 = create_chunk(combination['tablePath1'], df1)
    df2_chunk, num_partitions2 = create_chunk(combination['tablePath2'], df2)
    matching_values = []
    for i in range(num_partitions1):
        chunk1 = df1_chunk.get_partition(i)
        for j in range(num_partitions2):
            chunk2 = df2_chunk.get_partition(j)
            matching_values = getting_matching_result(chunk1, chunk2)
    return process_matching_result(matching_values, df1, df2)


@delayed
def create_chunk(filePath, dataframe):
    target_chunk_size_mb = 100
    file_size = os.path.getsize(filePath)
    num_partitions = (file_size // (target_chunk_size_mb * 1024 * 1024)) + 1
    divisions = [i * (file_size // num_partitions) for i in range(num_partitions + 1)]
    chunk = dataframe.repartition(divisions=divisions)
    return chunk, num_partitions


@delayed
def comparing_dataframes(combination_concat_df, combination, columnName1, columnName2):
    df1 = combination_concat_df[0][columnName1]
    df2 = combination_concat_df[1][columnName2]
    if not isinstance(df1, dd.Series) or not isinstance(df2, dd.Series):
        raise ValueError("Input columns must be Dask Series.")

    return comparingColumns(df1, df2, combination)
