import sys
import dask

import traceback

from crawler.bean import Validation
from crawler.service import DataFrameCreations


def print_or_store_result(combination, result):
    print(f"Result for combination {combination['taskId']}: {result}")


def startCrawling(combinations):
    try:
        validated_combination = dask.compute(Validation.start_validation(combinations))
        delayed_result = dask.compute(DataFrameCreations.process_flatFile(validated_combination))
        #final result loop and get the value

    except Exception as e:
        print(f"Error during computation: {e}")

# for combination, result in zip(combinations, results):
#     print_or_store_result(combination, result)
