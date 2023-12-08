import os
import io
import ftplib


class FileReaderUtils:

    @staticmethod
    def get_remote_csv_files_for_ftp(ftp, file_path):
        try:
            file_content = io.BytesIO()
            ftp.retrbinary(f"RETR {file_path}", file_content.write)
            file_content.seek(0)
            return [file_content]
        except ftplib.error_perm as e:
            print(f"FTP error: {e}")
            return []

    @staticmethod
    def get_remote_csv_files(sftp, file_path):
        try:
            csv_file = sftp.open(file_path)
            return [csv_file]
        except FileNotFoundError:
            print(f"File not found: {file_path}")
            return []
        except Exception as e:
            print(f"Error opening file: {e}")
            return []

    @staticmethod
    def get_local_csv_files(file_path):
        csv_files = []

        for root, dirs, files in os.walk(file_path):
            for file in files:
                if file.endswith(".csv"):
                    local_file_path = os.path.join(root, file)
                    csv_files.append(local_file_path)

        return csv_files


