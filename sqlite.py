import os
import logging
import sqlite3
import pandas as pd
from pathlib import Path
from noaa_scrape_data import *

class GoesSqlite:
    def __init__(self):

        noaa_scrape = Scrape_Data()

        database_file_name = 'scrape_data.db'
        self.database_file_path = os.path.join(os.path.dirname(__file__), database_file_name)

        ddl_file_name = 'geos18.sql'
        self.ddl_file_path = os.path.join(os.path.dirname(__file__), ddl_file_name)

        self.df = noaa_scrape.geos18_data()
        self.table_name = 'GEOS18'

    def create_database(self):
        with open(self.ddl_file_path, 'r') as sql_file:
            sql_script = sql_file.read()        
        db = sqlite3.connect(self.database_file_path)
        self.df.to_sql(self.table_name, db, if_exists = 'replace', index=False)
        cursor = db.cursor()
        cursor.executescript(sql_script)
        db.commit()
        db.close()

    def check_database_initilization(self):
        print(os.path.dirname(__file__))
        if Path(self.database_file_path).is_file():
            logging.info(f"Database exists, initilizing at : {self.database_file_path}")
            self.create_database()
        else:
            logging.info("Database file already exist") 

    def fetch_from_sql_into_df(self):
        db = sqlite3.connect(self.database_file_path)
        df1 = pd.read_sql_query("SELECT * FROM GEOS18", db)
        logging.info(df1)
        return df1

    def main(self):
        self.check_database_initilization()
        data = self.fetch_from_sql_into_df()
        return data


class NexradSqlite:
    def __init__(self):

        noaa_scrape = Scrape_Data()

        database_file_name = 'scrape_data.db'
        self.database_file_path = os.path.join(os.path.dirname(__file__), database_file_name)

        ddl_file_name = 'nexrad.sql'
        self.ddl_file_path = os.path.join(os.path.dirname(__file__), ddl_file_name)

        self.df = noaa_scrape.nexrad_data()
        self.table_name = 'NEXRAD'

    def create_database(self):
        with open(self.ddl_file_path, 'r') as sql_file:
            sql_script = sql_file.read()        
        db = sqlite3.connect(self.database_file_path)
        self.df.to_sql(self.table_name, db, if_exists = 'replace', index=False)
        cursor = db.cursor()
        cursor.executescript(sql_script)
        db.commit()
        db.close()

    def check_database_initilization(self):
        print(os.path.dirname(__file__))
        if Path(self.database_file_path).is_file():
            logging.info(f"Database exists, initilizing at : {self.database_file_path}")
            self.create_database()
        else:
            logging.info("Database file already exist") 

    def fetch_from_sql_into_df(self):
        db = sqlite3.connect(self.database_file_path)
        df1 = pd.read_sql_query("SELECT * FROM NEXRAD", db)
        logging.info(df1)
        return df1

    def main(self):
        self.check_database_initilization()
        data = self.fetch_from_sql_into_df()
        return data
