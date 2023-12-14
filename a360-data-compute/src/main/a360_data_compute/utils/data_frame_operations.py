import itertools
import traceback

import dask
import dask.dataframe as dd
from dask import delayed


@delayed
def count_matching_rows(chunk_1, chunk_2, column1, column2):
    # Perform merge on the specified columns
    merged_df = dd.merge(chunk_1, chunk_2, how='inner', left_on=column1, right_on=column2)

    # Count the number of matching rows
    matching_rows = len(merged_df)

    return matching_rows


def get_matching_count(first_df, second_df, column1, column2):
    # _row_count = first_df.shape[0].compute()
    # _row_count2 = second_df.shape[0].compute()
    global df_1, df_2
    try:
        actual_column1 = next((col for col in first_df.columns if col.lower() == column1.lower()), None)
        if actual_column1:
            df_1 = first_df.drop_duplicates(subset=actual_column1)
        else:
            raise Exception(f"Column '{column1}' not present in the first dataframe")

        actual_column2 = next((col for col in second_df.columns if col.lower() == column2.lower()), None)
        if actual_column2:
            df_2 = second_df.drop_duplicates(subset=actual_column2)
        else:
            raise Exception(f"Column '{column2}' not present in the second dataframe")
        total_row_count = df_1.shape[0].compute()
        total_row_count2 = df_2.shape[0].compute()

        chunk_size = '500MB'
        df_1 = df_1.repartition(partition_size=chunk_size)
        df_2 = df_2.repartition(partition_size=chunk_size)

        matching_counts = []

        for part_1, part_2 in itertools.product(df_1.to_delayed(), df_2.to_delayed()):
            matching_counts.append(count_matching_rows(part_1, part_2, actual_column1, actual_column2))

        total_matching_count = delayed(sum)(matching_counts)

        result = total_matching_count.compute()
        print(result)
        return result

    except Exception as partition_exception:
        print(f"Unexpected error: {partition_exception}")
        traceback.print_exc()

# def chunkify(df: pd.DataFrame, chunk_size: int):
#     start = 0
#     length = df.shape[0]
#
#     # If DF is smaller than the chunk, return the DF
#     if length <= chunk_size:
#         yield df[:]
#         return
#
#     # Yield individual chunks
#     while start + chunk_size <= length:
#         yield df[start:chunk_size + start]
#         start = start + chunk_size
#
#     df = df.chunk(1000)
#
#     # Yield the remainder chunk, if needed
#     if start < length:
# #         yield df[start:]
#   for partition_df_1_id in range(first_df.npartitions):
#             temp_df_1 = df1.get_partition(partition_df_1_id)
#             for partition_df_2_id in range(second_df.npartitions):
#                 temp_df_2 = df2.get_partition(partition_df_2_id)
#                 column_name_df1 = temp_df_1.columns[0]
#                 column_name_df2 = temp_df_2.columns[0]
#
#                 try:
#                     # Attempt to merge dataframes
#                     merged_df = merge_diff_col_df(column_name_df1, column_name_df2, temp_df_1, temp_df_2)
#
#                     # Get the matching count from the merged dataframe
#                     matching_count = merged_df.shape[0]
#                     matching_values.append(matching_count)
#
#                 except Exception as merge_exception:
#                     print(f"Error in merging dataframes: {merge_exception}")
#
#         total_matching_count = dask.compute(sum(matching_values))[0]
#         return total_matching_count
