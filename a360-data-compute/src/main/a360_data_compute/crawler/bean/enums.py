from enum import Enum


class PropertiesBean(Enum):
    REJECTION_LIMIT: float = 20
    APPROVAL_LIMIT: float = 80.0


class ApprovalStatus(Enum):
    APPROVED = 'APPROVED'
    PENDING = 'PENDING'
    REJECTED = 'REJECTED'


class AccuracyLevel(Enum):
    HIGH = 'HIGH'
    MEDIUM = 'MEDIUM'
    LOW = 'LOW'
    NOT_RELATED = 'NOT_RELATED'


class ConnectionType(Enum):
    LOCAL = 'Local_Storage'
    SFTP = 'sftp'
    FTP = 'ftp'


class File_Formate(Enum):
    CSV = 'csv'
    TXT = 'txt'
