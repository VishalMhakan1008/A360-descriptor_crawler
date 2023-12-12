import concurrent.futures
from collections import defaultdict

import dask
import duckdb
import numpy as np
import pandas as pd

from src.main.a360_data_compute.descriptor.bean.ColumnBean import ColumnBean
from src.main.a360_data_compute.descriptor.bean.DatabaseCommonMethods import DatabaseCommonMethods
from src.main.a360_data_compute.descriptor.bean.TableBean import TableBean
from src.main.a360_data_compute.descriptor.bean.enum.PortfolioConstants import PortfolioConstants
from src.main.a360_data_compute.utils.LogUtility import LogUtility


class MetadataProcessor:
    log_utility = LogUtility()

    @staticmethod
    def generate_metadata(csv_data, request_dto, table_bean):
        schema_name = request_dto.schema_name
        table_name = table_bean.name
        if csv_data is None:
            MetadataProcessor.log_utility.log_error("CSV data is None. Unable to generate metadata.")
            return None

        column_beans = []
        is_all_alphabet: bool

        try:
            requested_columns = [column.name for column in table_bean.columns]

            if requested_columns:
                csv_data = csv_data[requested_columns]

            delayed_columns = [
                dask.delayed(MetadataProcessor.process_column)(csv_data[column], column)
                for column in csv_data.columns
            ]

            computed_columns = dask.compute(*delayed_columns)
            contains_unstructured = any(column.is_unstructured for column in computed_columns)
            for computed_column in computed_columns:
                if computed_column is not None:
                    column_beans.append(computed_column)

            column_count = len(csv_data.columns)
            row_count = len(csv_data)
            probable_primary_key_size = MetadataProcessor.get_probable_primary_columns(computed_columns, row_count,
                                                                                       table_name)
            primary_key_size = MetadataProcessor.get_primary_key_size(computed_columns)

            table_bean = TableBean(column_count, row_count,
                                   {col: col_bean for col, col_bean in zip(csv_data.columns, computed_columns)},
                                   schema_name, table_name, probable_primary_key_size, contains_unstructured,
                                   primary_key_size)

            return table_bean

        except Exception as e:
            MetadataProcessor.log_utility.log_error(f"Error generating metadata: {e}")
            return None

    @staticmethod
    def get_probable_primary_columns(column_beans, row_count, table_name):
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for column in column_beans:
                futures.append(
                    executor.submit(
                        MetadataProcessor.probable_primary_key, column, row_count, table_name
                    )
                )

            concurrent.futures.wait(futures)

        primary_keys = [future.result() for future in futures if future.result() is not None]
        return len(primary_keys)

    @staticmethod
    def probable_primary_key(column, row_count, table_name):
        column.probable_primary = False
        column.is_unique_key = False
        column.probable_primary_for_crawl = False

        if column.distinct_row_count is not None and column.distinct_row_count != -1:
            try:
                num_of_nulls = column.null_row_count
                if (
                        num_of_nulls != -1
                        and row_count - column.distinct_row_count
                        <= PortfolioConstants.ACCEPTED_DUPLICATION_PERCENTAGE_CRAWL.value
                        * row_count
                        / 100
                        and num_of_nulls
                        <= PortfolioConstants.ACCEPTED_NULL_RECORDS_PERCENTAGE_CRAWL.value
                        * row_count
                        / 100
                ):
                    column.probable_primary_for_crawl = True
            except Exception as ex:
                MetadataProcessor.log_utility.log_warning(f"Exception in GetPrimaryColumns in Table ::  {table_name}",
                                                          ex)

            if column.primary_key:
                return None

            if MetadataProcessor.to_skip_for_primary_cols(column):
                return None

            try:
                num_of_nulls = column.null_row_count
                if (
                        num_of_nulls != -1
                        and row_count - column.distinct_row_count
                        <= PortfolioConstants.ACCEPTED_DUPLICATION_PERCENTAGE.value
                        * row_count
                        / 100
                        and num_of_nulls
                        <= PortfolioConstants.ACCEPTED_NULL_RECORDS_PERCENTAGE.value
                        * row_count
                        / 100
                ):
                    if column.data_type == "DATE" or column.data_type == "TIME":
                        column.is_unique_key = True
                    else:
                        column.probable_primary = True
            except Exception as ex:
                MetadataProcessor.log_utility.log_error(
                    f"Exception in GetPrimaryColumns in Table ::  {table_name}" + ex)

    @staticmethod
    def process_column(pandas_column, column):
        try:
            distinct_row_count = len(pandas_column.unique())
            null_row_count = pandas_column.isnull().sum().item()
            all_numeric = all(pd.to_numeric(pandas_column, errors='coerce').notna())
            contains_digit = any(char.isdigit() for s in pandas_column.dropna() for char in s)
            unique_count = len(pandas_column.unique())
            is_primary_key = unique_count == len(pandas_column)
            probable_primary_for_crawl = False
            is_date_column = False
            df = pd.DataFrame({column: pandas_column})
            duckdb_relation = duckdb.from_df(df)
            data_type = str(duckdb_relation[column].dtypes)
            if data_type in ['DATE', 'DATE_TIME', 'TIME']:
                is_date_column = True

            is_unstructured = DatabaseCommonMethods.is_unstructured(data_type)

            type_length = pandas_column.astype(str).apply(len).max()
            max_whitespace_count = pandas_column.apply(lambda x: x.count(' ') if isinstance(x, str) else 0).max().item()

            try:
                is_all_alphabet = all(isinstance(s, str) and s.isalpha() for s in pandas_column.dropna())
            except AttributeError:
                is_all_alphabet = False

            try:
                is_length_uniform = pandas_column.str.len().nunique() == 1
            except AttributeError:
                is_length_uniform = False
            is_high_frequency_char = False
            high_frequency_char_data = ""
            if is_length_uniform:
                for col_datum in pandas_column:
                    if isinstance(col_datum, (str, list, tuple, pd.Series, np.ndarray)):
                        if MetadataProcessor.max_occuring_char(col_datum) >= 0.6 * len(col_datum):
                            is_high_frequency_char = True
                            high_frequency_char_data = col_datum
                    else:
                        MetadataProcessor.log_utility.log_error(
                            "Invalid type for col_datum: {}".format(type(col_datum)))

        except Exception as e:
            MetadataProcessor.log_utility.log_error(f"Error generating metadata: {e}")
            return None

        return ColumnBean(column, data_type, distinct_row_count, null_row_count, all_numeric, is_all_alphabet,
                          is_primary_key, is_date_column, is_length_uniform, int(type_length), is_unstructured,
                          is_high_frequency_char, high_frequency_char_data, contains_digit,
                          max_whitespace_count, probable_primary_for_crawl)

    @staticmethod
    def to_skip_for_primary_cols(column):
        if DatabaseCommonMethods.is_unstructured(column.data_type):
            return True
        if (
                column.primary_key == "TRUE"
        ):
            return True
        return False

    @staticmethod
    def get_primary_key_size(column_beans):
        primary_key_count = sum(column.primary_key for column in column_beans)
        return primary_key_count

    @staticmethod
    def max_occuring_char(s):
        if isinstance(s, (str, np.str_)):
            char_map = defaultdict(int)
            for char in s:
                char_map[char] += 1
            return max(char_map.values(), default=0)
        else:
            return 0
