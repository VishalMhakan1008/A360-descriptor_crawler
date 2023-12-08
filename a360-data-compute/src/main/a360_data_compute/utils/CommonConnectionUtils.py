import ftplib
import paramiko as paramiko
import psycopg2


class CommonConnectionUtils:

    @staticmethod
    def get_postgres_connection(host, port, database, user, password):
        connection_str = f'host={host} port={port} dbname={database} user={user} password={password}'
        try:
            connection = psycopg2.connect(connection_str)
            print('Successfully connected to the PostgreSQL database')
            return connection
        except Exception as ex:
            print(f'Failed to connect: {ex}')
            return None

    @staticmethod
    def processSFTP(host, username, port, password):
        transport = paramiko.Transport(host, port)
        transport.connect(username, password)
        sftp = transport.open_sftp_client()
        return sftp

    @staticmethod
    def processFTP(host, password, port, username):
        ftp = ftplib.FTP()
        ftp.connect(host, port)
        ftp.login(username, password)
        return ftp

    @staticmethod
    def get_process_status(connection, process_id):
        status_query = "SELECT process_status FROM process_status WHERE process_id = %s;"
        try:
            with connection.cursor() as cursor:
                cursor.execute(status_query, (process_id,))
                status = cursor.fetchone()

                if status:
                    return status[0]
                else:
                    print(f"No status found for process_id {process_id}")
                    return None

        except Exception as ex:
            print(f"Error fetching status: {ex}")
            return None

    @staticmethod
    def close_connection(connection):
        if connection:
            connection.close()
