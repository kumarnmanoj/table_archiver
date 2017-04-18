import argparse
import sys
import logging

from os import path
from ConfigParser import ConfigParser
from pg_create_table import generate_create_table_for


def load_db_config_from_file(filepath):
    resolved_filepath = path.abspath(filepath)
    config = ConfigParser()

    config.read(resolved_filepath)

    dir(config)
    db_config = {}
    db_config["type"] = config.get("database","type","postgres")
    db_config["host"] = config.get("database","host","localhost")
    db_config["port"] = config.getint("database", "port")
    db_config["db"] = config.get("database", "db")
    db_config["user"] = config.get("database", "user")
    db_config["password"] = config.get("database", "password")

    return db_config 
    

if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("-c", "--config", help="Configuration file path in ini format")
    arg_parser.add_argument("-s", "--schema", help="Schema to which the table belongs")
    arg_parser.add_argument("-t", "--table", help="Table for which staging table DDL needs to be generated")
    arg_parser.add_argument("-n", "--name", help="Name of the end target table")

    args = arg_parser.parse_args()

    if not (args.config and args.schema and args.table and args.name):
        arg_parser.print_help()
        sys.exit(-1)

    db_config = load_db_config_from_file(args.config)
    db_config['target_table'] = args.name
    db_config['schema'] = args.schema
    db_config['table'] = args.table
    print generate_create_table_for(**db_config) 
