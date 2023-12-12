import json


class CurrentWorkingCombinationFF:
    def __init__(self, taskId, connectionType, tablePath1, tableName1, columnName1, column1ID, schema1ID, table1Size,
                 table1rowCount,
                 tablePath2, tableName2, columnName2, column2Id, schema2Id, table2Size, table2rowCount,
                 fileFormat, userName, password, host, port, ignoreQuotations, quoteCharacter, withStrictQuotes,
                 delimiter):
        self.taskId = taskId
        self.connectionType = connectionType
        self.tablePath1 = tablePath1
        self.tableName1 = tableName1
        self.columnName1 = columnName1
        self.column1Id = column1ID
        self.schema1Id = schema1ID
        self.table1Size = int(table1Size)
        self.table1rowCount = int(table1rowCount)

        self.tablePath2 = tablePath2
        self.tableName2 = tableName2
        self.columnName2 = columnName2
        self.column2Id = column2Id
        self.schema2Id = schema2Id
        self.table2Size = int(table2Size)
        self.table2rowCount = int(table2rowCount)

        self.fileFormat = fileFormat
        self.userName = userName
        self.password = password
        self.host = host
        self.port = port
        self.ignoreQuotations = ignoreQuotations
        self.quoteCharacter = quoteCharacter
        self.withStrictQuotes = withStrictQuotes
        self.delimiter = delimiter


class CrawlFlatfileRequestDTO:
    def __init__(self, type, processId, currentWorkingCombinationFF):
        self.type = type
        self.processId = int(processId)
        self.currentWorkingCombinationFF = [CurrentWorkingCombinationFF(**item) for item in currentWorkingCombinationFF]


class MatchingDTO:
    def __init__(self, forward_matching, reverse_matching, accuracy_level, approval_status, confidence_score):
        self.forward_matching = forward_matching
        self.reverse_matching = reverse_matching
        self.accuracy_level = accuracy_level
        self.approval_status = approval_status
        self.confidence_score = confidence_score

