from crawler.bean.EnumClass import ConnectionType
from crawler.bean.RequestDTO import CrawlFlatfileRequestDTO, CurrentWorkingCombinationFF
from crawler.service.dataframe_comparison import execute_combinations
from utils.FileReading import read_local_csv


def process_flatFile(crawl_flatfile_DTO: CrawlFlatfileRequestDTO, temp_object):
    list_of_combination_final_set = []
    for dto in crawl_flatfile_DTO.currentWorkingCombinationFF:
        #per 20 chunk
        print(dto.connectionType)
        print(ConnectionType.LOCAL.value)
        if dto.connectionType == ConnectionType.LOCAL.value:
            first_df = read_local_csv(dto.tablePath1,
                                      dto.delimiter,
                                      dto.quoteCharacter,
                                      dto.columnName1)
            print(first_df.dtypes)
            second_df = read_local_csv(dto.tablePath2,
                                       dto.delimiter,
                                       dto.quoteCharacter,
                                       dto.columnName2)
            print(second_df.dtypes)
            data_frames_sets = {
                "first_df": first_df,
                "second_df": second_df,
                "comb_dto": dto
            }
            list_of_combination_final_set.append(data_frames_sets)
    execute_combinations(list_of_combination_final_set, temp_object, crawl_flatfile_DTO)
