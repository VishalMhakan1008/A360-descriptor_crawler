
import sys
import dask

import traceback

from crawler.bean import Validation
from crawler.service import DataFrameCreations


def print_or_store_result(combination, result):
    print(f"Result for combination {combination['taskId']}: {result}")


def startCrawling(combinations):
    try:
        results = []

        validated_combination = Validation.start_validation(combinations)

        delayed_results = []
        for combination in validated_combination:
            delayed_result = DataFrameCreations.process_flatfile(combination)
            delayed_results.append(delayed_result)

        try:
            results = dask.compute(*delayed_results)
        except Exception as e:
            print(f"Error during computation: {e}")
    except Exception as e:
        print(e)
        traceback.print_exception(*sys.exc_info())

    # for combination, result in zip(combinations, results):
    #     print_or_store_result(combination, result)
