class TableBean:
    row_count: int

    def __init__(self, column_count, row_count, columns, schema_name, table_name, probable_primary_key_size, contains_unstructured, primary_key_size):
        self.table_name = table_name
        self.column_count = column_count
        self.row_count = row_count
        self.columns = columns
        self.schema_name = schema_name
        self.probable_primary_key_size = probable_primary_key_size
        self.contains_unstructured = contains_unstructured
        self.primary_key_size = primary_key_size

    def to_dict(self):
        column_list = []
        for col in self.columns.values():
            column_list.append(col.to_dict())

        return {
            "table_name": self.table_name,
            "column_count": self.column_count,
            "record_count": self.row_count,
            "columns": column_list,
            "schema_name": self.schema_name,
            "probable_primary_key_size": self.probable_primary_key_size,
            "contains_unstructured": self.contains_unstructured,
            "primary_key_size": self.primary_key_size
        }
