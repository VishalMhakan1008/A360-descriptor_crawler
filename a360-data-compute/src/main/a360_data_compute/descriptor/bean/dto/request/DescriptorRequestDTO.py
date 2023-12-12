
class DescriptorRequestDTO:
    def __init__(self, connection_dto, table_bean, columns, schema_name):
        self.connection_dto = connection_dto
        self.table_bean = table_bean
        self.columns = columns
        self.schema_name = schema_name
