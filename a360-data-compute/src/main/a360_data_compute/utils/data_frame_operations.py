import traceback

import dask
import dask.dataframe as dd


def get_matching_count(first_df, second_df):
    try:
        print(first_df.compute().shape[0])
        print(second_df.compute().shape[0])
        column1 = first_df.columns[0]
        column2 = second_df.columns[0]
        # merged_df = merge_diff_col_df(column1, column2, first_df, second_df)
        # print(merged_df.compute().shape[0])

        series1 = first_df[column1].compute()
        series2 = second_df[column2].compute()

        # Use isin method on pandas Series
        matching_rows_series = series1.isin(series2)

        # Convert the resulting pandas Series back to Dask Series if needed
        matching_rows_dask_series = dd.from_pandas(matching_rows_series, npartitions=first_df.npartitions)

        # Compute and print the shape of the resulting Dask Series
        print(matching_rows_dask_series.compute().shape[0])

        # Get the matching count from the merged dataframe

        # Drop duplicates in both dataframes

        non_null_df1 = first_df.drop_duplicates(subset=[column1])
        non_null_df2 = second_df.drop_duplicates(subset=[column2])
        print(non_null_df1.compute().shape[0])
        print(non_null_df2.compute().shape[0])

        # Repartition dataframes
        df1 = non_null_df1.repartition(npartitions=first_df.npartitions)
        df2 = non_null_df2.repartition(npartitions=second_df.npartitions)
        print(df1.compute().shape[0])
        print(df2.compute().shape[0])
        matching_values = []

        for partition_df_1_id in range(first_df.npartitions):
            temp_df_1 = df1.get_partition(partition_df_1_id)
            print("temp 1 df ")
            print(temp_df_1.compute().shape[0])
            for partition_df_2_id in range(second_df.npartitions):
                temp_df_2 = df2.get_partition(partition_df_2_id)
                print("temp 2 df ")
                print(temp_df_2.compute().shape[0])
                column_name_df1 = temp_df_1.columns[0]
                column_name_df2 = temp_df_2.columns[0]

                try:
                    # Attempt to merge dataframes
                    merged_df = merge_diff_col_df(column_name_df1, column_name_df2, temp_df_1, temp_df_2)
                    print(merged_df)

                    # Get the matching count from the merged dataframe
                    matching_count = merged_df.shape[0]
                    matching_values.append(matching_count)

                except Exception as merge_exception:
                    print(f"Error in merging dataframes: {merge_exception}")
                    traceback.print_exc()

        total_matching_count = dask.compute(sum(matching_values))[0]
        print(total_matching_count)
        return total_matching_count

    except Exception as partition_exception:
        print(f"Unexpected error: {partition_exception}")
        traceback.print_exc()


def merge_diff_col_df(column_name_df1, column_name_df2, temp_df_1, temp_df_2):
    # merged_df = dd.merge(temp_df_1, temp_df_2, how='inner', left_on=column_name_df1,
    #                      right_on=column_name_df2)
    merged_df = dd.merge(temp_df_1[[column_name_df1]].drop_duplicates(), temp_df_2[[column_name_df2]].drop_duplicates(),
                         how='inner',
                         left_on=column_name_df1, right_on=column_name_df2)

    return merged_df
