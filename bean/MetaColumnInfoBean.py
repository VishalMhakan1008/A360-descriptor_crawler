class MetaColumnInfoBean:
    def __init__(self, column_name, data_type, type_length):
        self.column_name = column_name
        self.data_type = data_type
        self.type_length = type_length

    def to_dict(self):
        return {
            "column_name": self.column_name,
            "data_type": self.data_type,
            "type_length": self.type_length
        }