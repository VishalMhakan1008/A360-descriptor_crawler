import os
from dask import dataframe as dd


def read_local_csv(table_path, delimiter, strict_quotes, quote_char, columnName=None, header=None):
    csv_files = []

    if not table_path.lower().endswith('.csv'):

        if os.path.exists(table_path):
            csv_files.append([
                os.path.join(table_path, file)
                for file in os.listdir(table_path)
                if file.endswith('.csv')
            ])
        else:
            raise FileNotFoundError(f"Files does not exist in the path: {table_path}")

    elif table_path.lower().endswith('.csv'):
        csv_files.append(table_path)

    else:
        raise ValueError("Unsupported file format. Supported formats are CSV.")

    if columnName:
        return dd.read_csv(csv_files,
                           delimiter=delimiter,
                           quotechar=quote_char,
                           header=header,
                           strict_quotes=strict_quotes,
                           names=[columnName],
                           dtype='object')
    else:
        return dd.read_csv(csv_files,
                           delimiter=delimiter,
                           quotechar=quote_char,
                           header=header,
                           strict_quotes=strict_quotes,
                           dtype='object')

# def read_local_xlsx():
#