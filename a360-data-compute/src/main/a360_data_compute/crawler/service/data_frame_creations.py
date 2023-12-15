import argparse

from src.main.a360_data_compute.crawler.bean.enums import ConnectionType, File_Formate
from src.main.a360_data_compute.crawler.bean.request_response_dtos import CrawlFlatfileRequestDTO, \
    CurrentWorkingCombinationFF
from src.main.a360_data_compute.crawler.service.dataframe_comparison import execute_combinations
from src.main.a360_data_compute.utils.FileReading import read_local_csv, read_ftp_csv_files, read_sftp_csv_files
import random


def read_ftp_csv_file(dto, temp_object):
    list_of_combination_final_set = []
    for comb_dto in dto.currentWorkingCombinationFF:
        first_df = read_ftp_csv_files(comb_dto.tablePath1,
                                      dto.delimiter,
                                      dto.quoteCharacter,
                                      dto.host,
                                      dto.userName,
                                      dto.password
                                      )
        second_df = read_ftp_csv_files(comb_dto.tablePath2,
                                       dto.delimiter,
                                       dto.quoteCharacter,
                                       dto.host,
                                       dto.userName,
                                       dto.password
                                       )
        data_frames_sets = {
            "first_df": first_df,
            "second_df": second_df,
            "comb_dto": comb_dto
        }
        list_of_combination_final_set.append(data_frames_sets)
    return execute_combinations(list_of_combination_final_set, temp_object, dto)


def read_local_csv_file(dto, temp_object):
    list_of_combination_final_set = []
    for comb_dto in dto.currentWorkingCombinationFF:
        first_df = read_local_csv(comb_dto.tablePath1,
                                  dto.delimiter,
                                  dto.quoteCharacter,
                                  )

        second_df = read_local_csv(comb_dto.tablePath2,
                                   dto.delimiter,
                                   dto.quoteCharacter,
                                   )
        data_frames_sets = {
            "first_df": first_df,
            "second_df": second_df,
            "comb_dto": comb_dto
        }
        list_of_combination_final_set.append(data_frames_sets)
    return execute_combinations(list_of_combination_final_set, temp_object, dto)


def read_sftp_csv_file(dto, temp_object):
    list_of_combination_final_set = []
    for comb_dto in dto.currentWorkingCombinationFF:
        first_df = read_sftp_csv_files(comb_dto.tablePath1,
                                       dto.delimiter,
                                       dto.quoteCharacter,
                                       dto.host,
                                       dto.userName,
                                       dto.password
                                       )

        second_df = read_sftp_csv_files(comb_dto.tablePath2,
                                        dto.delimiter,
                                        dto.quoteCharacter,
                                        dto.host,
                                        dto.userName,
                                        dto.password
                                        )
        data_frames_sets = {
            "first_df": first_df,
            "second_df": second_df,
            "comb_dto": comb_dto
        }
        list_of_combination_final_set.append(data_frames_sets)
    return execute_combinations(list_of_combination_final_set, temp_object, dto)


def process_flatFile(dto: CrawlFlatfileRequestDTO, temp_object):
    if dto.connectionType.casefold() == ConnectionType.LOCAL.value.casefold():
        if File_Formate.CSV.value.casefold() == dto.fileFormat.casefold():
            return read_local_csv_file(dto, temp_object)
        elif File_Formate.TXT.value.casefold() == dto.fileFormat.casefold():
            pass
        else:
            raise argparse.ArgumentTypeError("Invalid file format.")

    elif dto.connectionType.casefold() == ConnectionType.SFTP.value.casefold():
        if File_Formate.CSV.value.casefold() == dto.fileFormat.casefold():
            return read_sftp_csv_file(dto, temp_object)
        elif File_Formate.TXT.value.casefold() == dto.fileFormat.casefold():
            pass
        else:
            raise argparse.ArgumentTypeError("Invalid file format.")

    elif dto.connectionType.casefold() == ConnectionType.FTP.value.casefold():
        if File_Formate.CSV.value.casefold() == dto.fileFormat.casefold():
            return read_ftp_csv_file(dto, temp_object)
        elif File_Formate.TXT.value.casefold() == dto.fileFormat.casefold():
            pass
        else:
            raise argparse.ArgumentTypeError("Invalid file format.")
    else:
        raise argparse.ArgumentTypeError("Invalid connection type")

