class DatabaseCommonMethods:
    @staticmethod
    def is_unstructured(col_type):
        try:
            col_type_upper = col_type.upper()
            unstructured_types = [
                "BLOB", "TINYBLOB", "TINY BLOB", "MEDIUMBLOB", "MEDIUM BLOB", "LONGBLOB", "LONG BLOB",
                "VARBINARY", "VAR BINARY", "NVARBINARY", "NVAR BINARY", "N VAR BINARY", "IMAGE",
                "LONGNVARBINARY", "LONG NVAR BINARY", "LONG N VAR BINARY", "LONGVARBINARY", "LONG VARBINARY",
                "PHOTO", "RAW", "LONG RAW", "LONGRAW", "PICTURE", "TEXT", "NTEXT", "XML", "CLOB", "ROWID"
            ]
            return col_type_upper in unstructured_types
        except Exception as e:
            return False

    @staticmethod
    def get_quote_identifier(con, db_type):
        try:
            return con.getMetaData().getIdentifierQuoteString()
        except Exception as e:
            if db_type in ["SQL", "ORACLE", "ORACLE_SERVICE", "AS400", "DB2", "DB2_MAINFRAME"]:
                return "\""
            elif db_type == "MYSQL":
                return "`"
            else:
                return "\""

    @staticmethod
    def is_allowed_for_min_max(column_type):
        if column_type.upper().find("VARCHAR") != -1:
            return False
        return DatabaseCommonMethods.is_allowed_type(column_type)

    @staticmethod
    def is_allowed_type(data_type):
        allowed_types = [
            "BLOB", "TINYBLOB", "MEDIUMBLOB", "LONGBLOB", "VARBINARY", "NVARBINARY", "IMAGE", "PHOTO", "BIT",
            "BOOLEAN", "MONEY", "CURRENCY", "SMALLMONEY", "BINARY_BOUBLE", "BINARY_FLOAT", "DOUBLE_PRECISION",
            "DOUBLE PRECISION", "BINARY VARYING", "TIME", "TIMESTAMP", "INTERVAL", "TIMESTAMP WITH LOCAL TIME ZONE",
            "TIMESTAMP WITH TIME ZONE", "DATETIME", "DATETIME2", "SMALLDATETIME", "DATETIMEOFFSET", "DATE", "RAW",
            "LONG RAW", "BIGINT", "BOOL", "CODE", "DECIMAL", "FLOAT", "NUMERIC() IDENTITY", "MESSAGE", "TINYINT",
            "TEXT", "XML", "CLOB", "GEOMETRY", "NTEXT", "ROWID"
        ]
        return data_type.upper() not in allowed_types

    @staticmethod
    def check_unstructured_data_type(col_type):
        try:
            unstructured_types = [
                "BLOB", "TINYBLOB", "TINY BLOB", "MEDIUMBLOB", "MEDIUM BLOB", "LONGBLOB", "LONG BLOB",
                "VARBINARY", "VAR BINARY", "NVARBINARY", "NVAR BINARY", "N VAR BINARY", "IMAGE",
                "LONGNVARBINARY", "LONG NVAR BINARY", "LONG N VAR BINARY", "LONGVARBINARY", "LONG VARBINARY",
                "PHOTO", "RAW", "LONG RAW", "LONGRAW", "PICTURE", "TEXT", "NTEXT", "XML", "CLOB", "ROWID",
                "GEOMETRY", "MDSYS.SDO_GEOMETRY", "LINE", "POINT", "POLYGON", "INET", "CIDR", "NCLOB"
            ]
            return col_type.upper() in unstructured_types
        except Exception as e:
            return False

    @staticmethod
    def check_data_type_for_length(col_type):
        try:
            length_types = ["TIMESTAMP", "DATE", "BINARY", "BOOLEAN", "BOOL"]
            return col_type.upper() in length_types
        except Exception as e:
            return False

    @staticmethod
    def check_data_type_for_numeric_contains(col_type):
        try:
            numeric_types = [
                "INTEGER", "INT", "INT4", "INT2", "FLOAT8", "TINYINT", "BIGINT", "TIME", "TIMESTAMP", "REAL",
                "DOUBLE", "DOUBLE PRECISION", "FLOAT", "SMALLINT", "SERIAL", "BIGSERIAL", "MEDIUMINT", "BIT", "YEAR"
            ]
            return col_type.upper() in numeric_types
        except Exception as e:
            return False
