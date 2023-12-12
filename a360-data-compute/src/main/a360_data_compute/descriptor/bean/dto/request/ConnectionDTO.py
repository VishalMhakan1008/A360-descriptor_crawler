class ConnectionDTO:
    def __init__(self, file_path, file_format, delimiter, connection_type, metadata_file_path, user_name, password, host, port,
                 ignore_quotations, with_strict_quotes, is_meta_info_available, quote_character):
        self.file_path = file_path
        self.file_format = file_format
        self.delimiter = delimiter
        self.connection_type = connection_type
        self.metadata_file_path = metadata_file_path
        self.user_name = user_name
        self.password = password
        self.host = host
        self.port = port
        self.ignore_quotations = ignore_quotations
        self.with_strict_quotes = with_strict_quotes
        self.is_meta_info_available = is_meta_info_available
        self.quote_character = quote_character
