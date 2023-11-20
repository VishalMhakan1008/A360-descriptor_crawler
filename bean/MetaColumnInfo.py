class MetaColumnInfo:
    def __init__(self, column, data_type, length, date_format=None, date_time_format=None):
        self.column = column
        self.data_type = data_type
        self.length = length
        self.date_format = date_format
        self.date_time_format = date_time_format