import itertools
import traceback


import dask.dataframe as dd
from dask import delayed


@delayed
def count_matching_rows(chunk_1, chunk_2, column1, column2):
    merged_df = dd.merge(chunk_1, chunk_2, how='inner', left_on=column1, right_on=column2)

    matching_rows = len(merged_df)

    return matching_rows


def get_matching_count(first_df, second_df, column1, column2):
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

        # chunk size determine  logic
        chunk_size = '275MB'
        df_1 = df_1.repartition(partition_size=chunk_size)
        df_2 = df_2.repartition(partition_size=chunk_size)

        matching_counts = []

        for part_1, part_2 in itertools.product(df_1.to_delayed(), df_2.to_delayed()):
            matching_counts.append(count_matching_rows(part_1, part_2, actual_column1, actual_column2))

        total_matching_count = delayed(sum)(matching_counts)

        result = total_matching_count.compute()
        return result

    except Exception as partition_exception:
        print(f"Unexpected error: {partition_exception}")
        traceback.print_exc()
