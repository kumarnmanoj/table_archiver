import psycopg2

from datetime import datetime, timedelta

class Archiver(object):
    _DATE_CONFIG_SUFFIX = {
        "d": "days",
        "m": "minutes",
        "M": "months"
    }

    _PG_DATE_FORMAT = "%Y-%m-%d"

    def __init__(self, archival_config):
        self.archival_config = archival_config


    def archive(self):
        db_config = self.archival_config.db_config
        tables_config = self.archival_config.tables_config

        connect_parameters = {
            "host": db_config.host,
            "port": db_config.port,
            "user": db_config.user,
            "password": db_config.password,
            "database": db_config.db
        }

        with psycopg2.connect(**connect_parameters) as conn:
            for table_config in tables_config:
                cursor = conn.cursor()
                self._archive_table(table_config, cursor)

    def _get_date_before(self, interval):
        interval_type = interval[-1:]
        interval_duration = int(interval[:-1])
        interval_parameters = {
            self._DATE_CONFIG_SUFFIX[interval_type]: interval_duration
        }

        return datetime.now() - timedelta(**interval_parameters)
        

    def _archive_table(self, table_config, cursor):
        print "Archiving table %s.%s" % (table_config.schema,table_config.table_name)
        cursor.execute("BEGIN;")
        ### Truncate staging table
        stage_table = "%s.%s_stage" % (table_config.schema, table_config.table_name)
        stage_truncate_query = "TRUNCATE %s;" % stage_table

        print stage_truncate_query
        cursor.execute(stage_truncate_query)
    
        ### Move data to staging table
        before_date_string = self._get_date_before(table_config.duration).strftime(self._PG_DATE_FORMAT)

        stage_move_query = """
            WITH moved_rows AS (
                DELETE FROM %s
                WHERE
                    "%s" < '%s'
                RETURNING *
            )
            INSERT INTO %s
            SELECT * FROM moved_rows;
        """ % (table_config.name, table_config.comparision_column, before_date_string, stage_table)

        print stage_move_query
        cursor.execute(stage_move_query)

        ### Remove duplicates from the archive table
        archive_table = "%s.%s_archive" % (table_config.schema, table_config.table_name)

        remove_archive_duplicate_query = """
            DELETE FROM %s 
                WHERE id in (SELECT id FROM %s);
        """ % (archive_table, stage_table)

        print remove_archive_duplicate_query	
        cursor.execute(remove_archive_duplicate_query)

        ### Move data to the archive table from the stage table
        archive_move_query = """
            INSERT INTO %s SELECT * FROM %s;
        """ % (archive_table, stage_table)
	
        print archive_move_query
        cursor.execute(archive_move_query)

        ### Truncate staging table
        print stage_truncate_query
        cursor.execute(stage_truncate_query)

        cursor.execute("COMMIT")
