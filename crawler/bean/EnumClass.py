from enum import Enum


class PropertiesBean(Enum):
    REJECTION_LIMIT = 20
    APPROVAL_LIMIT = 80


class ApprovalStatus(Enum):
    APPROVED = "APPROVED"
    PENDING = "PENDING"
    REJECTED = "REJECTED"


class AccuracyLevel(Enum):
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    NOT_RELATED = "NOT_RELATED"


class ConnectionType(Enum):
    LOCAL = "Local Storage"
    SFTP = "sftp"
    FTP = "ftp"
