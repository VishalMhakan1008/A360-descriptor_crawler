import dask

from src.main.a360_data_compute.crawler.bean import validation
from src.main.a360_data_compute.crawler.service import data_frame_creations


def startCrawling(crawl_flatfile_DTO, temp_object):
    try:
        # validated_combination = Validation.start_validation(crawl_flatfile_DTO)
        return data_frame_creations.process_flatFile(crawl_flatfile_DTO, temp_object)
    except Exception as e:
        print(f"Error during computation: {e}")
