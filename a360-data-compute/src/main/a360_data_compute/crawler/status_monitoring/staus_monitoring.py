from datetime import datetime, timedelta
import time


class Process_monitoring:
    end_time = 0.0
    result_path: str

    def __init__(self, process_id, status):
        self.process_id = process_id
        self.status = status
        self.start_time = time.time()

    def to_dict(self):
        return {
            "process_id": self.process_id,
            "start_time": datetime.fromtimestamp(self.start_time).strftime('%F %T.%f')[:-3],
            "end_time": datetime.fromtimestamp(self.end_time).strftime('%F %T.%f')[:-3],
            "time_taken": str(timedelta(seconds=self.end_time - self.start_time))
        }

    def to_dict_result(self):
        return {
            "process_id": self.process_id,
            "start_time": datetime.fromtimestamp(self.start_time).strftime('%F %T.%f')[:-3],
            "end_time": datetime.fromtimestamp(self.end_time).strftime('%F %T.%f')[:-3],
            "time_taken": str(timedelta(seconds=self.end_time - self.start_time)),
            "result": self.result_path
        }


 # matching_result = FlatFileMatchingResultResponseDTO(dto.taskId, dto.column1Id,
 #                                                        dto.schema1Id, dto.tableName1,
 #                                                        dto.column2Id, dto.column2Id,
 #                                                        dto.schema2Id, dto.tablePath2,
 #                                                        combination_result['forward_matching'],
 #                                                        combination_result['reverse_matching'],
 #                                                        combination_result['approvalStatus'],
 #                                                        combination_result['accuracyLevel']
 #                                                        )
 #    return matching_result