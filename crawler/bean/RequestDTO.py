import json


class CurrentWorkingCombinationFF:
    def __init__(self, taskId, connectionType, schemaName1, tablePath1, tableName1, columnName1, column1ID, schema1ID,
                 schemaName2, databaseName2, tablePath2, tableName2, columnName2, column2Id, schema2Id,
                 fileFormat, userName, password, host, port, ignoreQuotations, quoteCharacter, withStrictQuotes,
                 delimiter):
        self.taskId = taskId
        self.connectionType = connectionType
        self.schemaName1 = schemaName1
        self.tablePath1 = tablePath1
        self.tableName1 = tableName1
        self.columnName1 = columnName1
        self.column1Id = column1ID
        self.schema1Id = schema1ID

        self.schemaName2 = schemaName2
        self.databaseName2 = databaseName2
        self.tablePath2 = tablePath2
        self.tableName2 = tableName2
        self.columnName2 = columnName2
        self.column2Id = column2Id
        self.schema2Id = schema2Id

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
        self.processId = processId
        self.currentWorkingCombinationFF = [CurrentWorkingCombinationFF(**item) for item in currentWorkingCombinationFF]


# class CrawlFlatfileResponseDTO:
#     def __init__(self, processId, flatfileMatchingResultResponse):
#         self.processId = processId
#         self.matching_result = flatfileMatchingResultResponse
#
#
# class FlatFileMatchingResultResponseDTO:
#     def __init__(self, taskId, column1Id, schema1Id, table1, column2Id, schema2Id, table2, forwardMatch, reverseMatch,
#                  confidenceScore,
#                  approvalStatus, accuracyLevel):
#         self.taskId = taskId
#         self.column1Id = column1Id
#         self.schema1Id = schema1Id
#         self.table1 = table1
#         self.column2Id = column2Id
#         self.schema2Id = schema2Id
#         self.table2 = table2
#         self.forwardMatch = forwardMatch
#         self.reverseMatch = reverseMatch
#         self.confidenceScore = confidenceScore
#         self.approvalStatus = approvalStatus
#         self.accuracyLevel = accuracyLevel
#
#     def __str__(self):
#         return (
#             f"taskId: {self.taskId}, column1Id: {self.column1Id}, schema1Id: {self.schema1Id}, "
#             f"table1: {self.table1}, column2Id: {self.column2Id}, schema2Id: {self.schema2Id}, "
#             f"table2: {self.table2}, forwardMatch: {self.forwardMatch}, reverseMatch: {self.reverseMatch}, "
#             f"confidenceScore: {self.confidenceScore}, approvalStatus: {self.approvalStatus}, "
#             f"accuracyLevel: {self.accuracyLevel}"
#         )
