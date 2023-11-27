import time
from datetime import datetime, timedelta


class Process:
    end_time = 0.0
    result_path: str
    def __init__(self, process_id, future, status):
        self.process_id = process_id
        self.future = future
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