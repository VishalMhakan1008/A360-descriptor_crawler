import os
import traceback
from pandas.errors import EmptyDataError, ParserError
from dask import dataframe as dd
from pandas.errors import EmptyDataError, ParserError


def read_local_csv(table_path, delimiter, quote_char, columnName=None, header=None):
    if not table_path.lower().endswith('.csv'):
        global csv_files
        if os.path.exists(table_path):
            csv_files = [
                os.path.join(table_path, file)
                for file in os.listdir(table_path)
                if file.endswith('.csv')
            ]
        else:
            raise FileNotFoundError(f"Files does not exist in the path: {table_path}")

    elif table_path.lower().endswith('.csv'):
        csv_files = table_path

    else:
        raise ValueError("Unsupported file format. Supported formats are CSV.")

    try:
        if columnName:
            files = csv_files
            return dd.read_csv(
                csv_files,
                delimiter=delimiter,
                quotechar=quote_char,
                header=1,
                names=[columnName],
                dtype='object'
            )
        else:
            return dd.read_csv(
                csv_files,
                delimiter=delimiter,
                quotechar=quote_char,
                header=1,
                dtype='object'
            )
    except EmptyDataError as e:
        raise EmptyDataError(f"Error: Empty CSV file - {str(e)}")
    except ParserError as e:
        raise ParserError(f"Error: Parsing issue in CSV file - {str(e)}")
    except UnicodeDecodeError as e:
        raise UnicodeDecodeError(f"Error: Unicode decoding issue - {str(e)}")
    except Exception as e:
        traceback.print_exc()
        raise Exception(f"An unexpected error occurred: {str(e)}")
# def read_local_xlsx():
#
