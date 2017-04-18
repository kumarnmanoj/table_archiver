class DB(object):
    def __init__(self, config):
        self.type = config.get("database", "type", "postgres")
        self.host = config.get("database", "host", "localhost")
        self.port = config.getint("database", "port")
        self.db   = config.get("database", "db")
        self.user = config.get("database", "user")
        self.password = config.get("database", "password")

class Table(object):
    def __init__(self, table_config):
        """ 
            table_config format: schema.tablename:date_comparison_column:duration
            
            duration format: 3d (for 3 days), 3m (for 3 minutes), 3M (for 3 months)
        """              

        configs = table_config.split(":")
        table_configs = configs[0].split(".")
        self.name = configs[0]
        self.schema = table_configs[0]
        self.table_name = table_configs[1]
        self.comparision_column = configs[1]
        self.duration = configs[2]

class ArchiveConfig(object):
    def __init__(self, db_config, tables_config):
        self.db_config = db_config
        self.tables_config = tables_config
