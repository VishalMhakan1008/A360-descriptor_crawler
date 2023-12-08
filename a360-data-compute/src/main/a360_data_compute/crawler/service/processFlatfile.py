import sys
import dask

import traceback

from src.main.a360_data_compute.crawler.bean.Validation import Validation
from src.main.a360_data_compute.crawler.service.DataFrameCreations import DataFrameCreation


class ProcessFlatFile:
    @staticmethod
    def print_or_store_result(combination, result):
        print(f"Result for combination {combination['taskId']}: {result}")

    results = None

    def startCrawling(self, combinations):
        global results

        try:
            results = []

            validated_combination = Validation.start_validation(combinations)

            delayed_results = []
            for combination in validated_combination:
                delayed_result = DataFrameCreation.process_flatfile(combination)
                delayed_results.append(delayed_result)

            try:
                results = dask.compute(*delayed_results)
            except Exception as e:
                print(f"Error during computation: {e}")
        except Exception as e:
            print(e)
            traceback.print_exception(*sys.exc_info())

        for combination, result in zip(combinations, results):
            self.print_or_store_result(combination, result)
