import ftplib

import paramiko


class FlatFileConnection:

    def establishConnection(self, connection_dto):
        connection_type = connection_dto.get('connectionType')
        if connection_type == 'sftp':
            self.establishSFTPConnection(connection_dto)
        elif connection_type == 'ftp':
            self.establishSFTPConnection(connection_dto)
        else:
            raise ValueError("Invalid connection type")

    @staticmethod
    def establishSFTPConnection(connection_dto):
        transport = paramiko.Transport((connection_dto['host'], int(connection_dto['port'])))
        transport.connect(username=connection_dto['userName'], password=connection_dto['password'])
        sftp = transport.open_sftp_client()
        return sftp

    @staticmethod
    def establish_ftp_connection(connection_dto):
        ftp = ftplib.FTP()
        ftp.connect(connection_dto.get('host'), int(connection_dto.get('port')))
        ftp.login(connection_dto.get('username'), connection_dto.get('password'))
        return ftp
