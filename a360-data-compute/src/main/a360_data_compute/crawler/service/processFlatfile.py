import dask

from crawler.bean import Validation
from crawler.service import DataFrameCreations


def startCrawling(crawl_flatfile_DTO, temp_object):
    try:
        # validated_combination = Validation.start_validation(crawl_flatfile_DTO)
        DataFrameCreations.process_flatFile(crawl_flatfile_DTO, temp_object)
    except Exception as e:
        print(f"Error during computation: {e}")
