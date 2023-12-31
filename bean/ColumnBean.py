class ColumnBean:
    distinct_row_count: int
    null_row_count: int
    all_numeric: bool
    all_alphabet: bool
    primary_key: bool
    is_date_column: bool
    is_length_uniform: bool
    type_length: int

    def __init__(self, column_name, data_type, distinct_row_count, null_row_count, all_numeric, all_alphabet, primary_key, is_date_column, is_length_uniform,type_length):
        self.column_name = column_name
        self.data_type = data_type
        self.distinct_row_count = distinct_row_count
        self.null_row_count = null_row_count
        self.all_numeric = all_numeric
        self.all_alphabet = all_alphabet
        self.primary_key = primary_key
        self.is_date_column = is_date_column
        self.is_length_uniform = is_length_uniform
        self.type_length = type_length

    def to_dict(self):
        return {
            "column_name": self.column_name,
            "data_type": self.data_type,
            'distinct_row_count': self.distinct_row_count,
            'null_row_count': self.null_row_count,
            'all_numeric': self.all_numeric,
            'all_alphabet': self.all_alphabet,
            'primary_key': self.primary_key,
            'is_date_column': self.is_date_column,
            'is_length_uniform': self.is_length_uniform
        }
