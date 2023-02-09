import os
import logging
import sqlite3
import pandas as pd
from pathlib import Path
from noaa_scrape_data import *
import streamlit as st

class GoesSqlite:
    def __init__(self):

        database_file_name = 'scrape_data.db'
        self.database_file_path = os.path.join(os.path.dirname(__file__), database_file_name)

        ddl_file_name = 'geos18.sql'
        self.ddl_file_path = os.path.join(os.path.dirname(__file__), ddl_file_name)

        self.df = pd.read_csv('geos18_data.csv', dtype = str)
        self.table_name = 'GEOS18'
    
    @st.cache
    def create_database(self):
        with open(self.ddl_file_path, 'r') as sql_file:
            sql_script = sql_file.read()        
        db = sqlite3.connect(self.database_file_path)
        self.df.to_sql(self.table_name, db, if_exists = 'replace', index=False)
        cursor = db.cursor()
        cursor.executescript(sql_script)
        db.commit()
        db.close()

    @st.cache
    def check_database_initilization(self):
        print(os.path.dirname(__file__))
        if Path(self.database_file_path).is_file():
            logging.info(f"Database exists, initilizing at : {self.database_file_path}")
            self.create_database()
        else:
            logging.info("Database file already exist") 

    @st.cache
    def fetch_from_sql_into_df(self):
        db = sqlite3.connect(self.database_file_path)
        df1 = pd.read_sql_query("SELECT * FROM GEOS18", db)
        logging.info(df1)
        return df1

    @st.cache
    def main(self):
        self.check_database_initilization()
        data = self.fetch_from_sql_into_df()
        return data


class NexradSqlite:
    def __init__(self):

        database_file_name = 'scrape_data.db'
        self.database_file_path = os.path.join(os.path.dirname(__file__), database_file_name)

        ddl_file_name = 'nexrad.sql'
        self.ddl_file_path = os.path.join(os.path.dirname(__file__), ddl_file_name)

        self.df = pd.read_csv('nexrad_data.csv', dtype = str)
        self.table_name = 'NEXRAD'

    @st.cache
    def create_database(self):
        with open(self.ddl_file_path, 'r') as sql_file:
            sql_script = sql_file.read()        
        db = sqlite3.connect(self.database_file_path)
        self.df.to_sql(self.table_name, db, if_exists = 'replace', index=False)
        cursor = db.cursor()
        cursor.executescript(sql_script)
        db.commit()
        db.close()

    @st.cache
    def check_database_initilization(self):
        print(os.path.dirname(__file__))
        if Path(self.database_file_path).is_file():
            logging.info(f"Database exists, initilizing at : {self.database_file_path}")
            self.create_database()
        else:
            logging.info("Database file already exist") 

    @st.cache
    def fetch_from_sql_into_df(self):
        db = sqlite3.connect(self.database_file_path)
        df1 = pd.read_sql_query("SELECT * FROM NEXRAD", db)
        logging.info(df1)
        return df1

    @st.cache
    def main(self):
        self.check_database_initilization()
        data = self.fetch_from_sql_into_df()
        return data
