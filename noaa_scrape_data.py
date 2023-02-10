import os
import boto3
import logging
import pandas as pd
from dotenv import load_dotenv
import streamlit as st

class Scrape_Data:
    def __init__(self):
        load_dotenv()
        
        LOGLEVEL = os.environ.get('LOGLEVEL', 'INFO').upper()
        logging.basicConfig(
            format='%(asctime)s %(levelname)-8s %(message)s',
            level=LOGLEVEL,
            datefmt='%Y-%m-%d %H:%M:%S',
            filename='logs.log')

        self.s3client = boto3.client('s3',
                            region_name='us-east-1',
                            aws_access_key_id = os.environ.get('AWS_ACCESS_KEY'),
                            aws_secret_access_key = os.environ.get('AWS_SECRET_KEY')
                            )
        
        self.geos_bucket_name = "noaa-goes18"
        self.geos18_data_dict = {'ID': [], 'Product_Name': [], 'Year': [], 'Day': [], 'Hour': []}
        self.nexrad_bucket_name = "noaa-nexrad-level2"
        self.nexrad_data_dict = {'ID': [], 'Year': [], 'Month': [], 'Day': [], 'NexRad Station Code': []}

    def geos18_data(self):
        logging.info('Scraping GOES18 Metadata into df')
        id = 1
        prefix = "ABI-L1b-RadC/"
        result = self.s3client.list_objects(Bucket = self.geos_bucket_name, Prefix = prefix, Delimiter = '/')
        
        for i in result.get('CommonPrefixes'):
            path = i.get('Prefix').split('/')
            prefix_2 = prefix + path[-2] + "/"
            sub_folder = self.s3client.list_objects(Bucket = self.geos_bucket_name, Prefix = prefix_2, Delimiter = '/')
            
            for j in sub_folder.get('CommonPrefixes'):
                sub_path = j.get('Prefix').split('/')
                prefix_3 = prefix_2 + sub_path[-2] + "/"
                sub_sub_folder = self.s3client.list_objects(Bucket = self.geos_bucket_name, Prefix = prefix_3, Delimiter = '/')
                
                for k in sub_sub_folder.get('CommonPrefixes'):
                    sub_sub_path = k.get('Prefix').split('/')
                    sub_sub_path = sub_sub_path[:-1]
                    self.geos18_data_dict['ID'].append(id)
                    self.geos18_data_dict['Product_Name'].append(sub_sub_path[0])
                    self.geos18_data_dict['Year'].append(sub_sub_path[1])
                    self.geos18_data_dict['Day'].append(sub_sub_path[2])
                    self.geos18_data_dict['Hour'].append(sub_sub_path[3])
                    id += 1
        
        geos18_data = pd.DataFrame(self.geos18_data_dict)
        geos18_data.to_csv('geos18_data.csv', index = False, na_rep = 'Unknown', encoding = 'utf-8')
        logging.info('Loaded scraped GOES18 metadata')
        return geos18_data

    def nexrad_data(self):
        logging.info('Scraping NEXRAD Metadata into df')
        id = 1
        years = ['2022','2023']
        for year in years:
            prefix = year + '/'
            result = self.s3client.list_objects(Bucket = self.nexrad_bucket_name, Prefix = prefix, Delimiter = '/')
            
            for i in result.get('CommonPrefixes'):
                path = i.get('Prefix').split('/')
                prefix_2 = prefix + path[-2] + "/"
                sub_folder = self.s3client.list_objects(Bucket = self.nexrad_bucket_name, Prefix = prefix_2, Delimiter = '/')
                
                for j in sub_folder.get('CommonPrefixes'):
                    sub_path = j.get('Prefix').split('/')
                    prefix_3 = prefix_2 + sub_path[-2] + "/"
                    sub_sub_folder = self.s3client.list_objects(Bucket = self.nexrad_bucket_name, Prefix = prefix_3, Delimiter = '/')

                    for k in sub_sub_folder.get('CommonPrefixes'):
                        sub_sub_path = k.get('Prefix').split('/')
                        sub_sub_path = sub_sub_path[:-1]
                        self.nexrad_data_dict['ID'].append(id)
                        self.nexrad_data_dict['Year'].append(sub_sub_path[0])
                        self.nexrad_data_dict['Month'].append(sub_sub_path[1])
                        self.nexrad_data_dict['Day'].append(sub_sub_path[2])
                        self.nexrad_data_dict['NexRad Station Code'].append(sub_sub_path[3])
                        id += 1
        
        nexrad_data = pd.DataFrame(self.nexrad_data_dict)
        nexrad_data.to_csv('nexrad_data.csv', index = False, na_rep = 'Unknown', encoding = 'utf-8')
        logging.info('Loaded scraped NEXRAD metadata')
        return nexrad_data