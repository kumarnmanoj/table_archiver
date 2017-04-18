import argparse
import sys

from os import path
from ConfigParser import ConfigParser
from archival_configs import *
from archiver import *

def load_archive_config_from(config_file_path):
    config_file_path = path.abspath(config_file_path)
    config = ConfigParser()

    config.read(config_file_path)

    db_config = DB(config)
    raw_tables_config = config.get("archive", "tables")
    tables_config = map(lambda rtc: Table(rtc), raw_tables_config.split(","))
    
    return ArchiveConfig(db_config, tables_config)

if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("-c", "--config", help="Configuration file(in ini part) path")

    args = arg_parser.parse_args()
    
    archival_config = load_archive_config_from(args.config)
    Archiver(archival_config).archive()

