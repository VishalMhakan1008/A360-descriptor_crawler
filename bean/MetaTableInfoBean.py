class MetaTableInfoBean:
    def __init__(self, schema_name, table_name, columns):
        self.schema_name = schema_name
        self.table_name = table_name
        self.columns = columns

    def to_dict(self):
        return {
            "schema_name": self.schema_name,
            "table_name": self.table_name,
            "columns": [column.to_dict() for column in self.columns]
        }
