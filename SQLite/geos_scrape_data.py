import os
import boto3
import logging
from dotenv import load_dotenv
import pandas as pd
from pathlib import Path

load_dotenv()

LOGLEVEL = os.environ.get('LOGLEVEL', 'INFO').upper()
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=LOGLEVEL,
    datefmt='%Y-%m-%d %H:%M:%S',
    filename='logs.log')

s3client = boto3.client('s3',
                        region_name='us-east-1',
                        aws_access_key_id = os.environ.get('AWS_ACCESS_KEY'),
                        aws_secret_access_key = os.environ.get('AWS_SECRET_KEY')
                        )

geos_bucket_name = "noaa-goes18"

goes18_data_dict = {'ID': [], 'Product_Name': [], 'Year': [], 'Day': [], 'Hour': []}

def goes18_data():
    id = 1
    prefix = "ABI-L1b-RadC/"
    result = s3client.list_objects(Bucket = geos_bucket_name, Prefix = prefix, Delimiter = '/')
    
    for i in result.get('CommonPrefixes'):
        path = i.get('Prefix').split('/')
        prefix_2 = prefix + path[-2] + "/"
        sub_folder = s3client.list_objects(Bucket = geos_bucket_name, Prefix = prefix_2, Delimiter = '/')
        
        for j in sub_folder.get('CommonPrefixes'):
            sub_path = j.get('Prefix').split('/')
            prefix_3 = prefix_2 + sub_path[-2] + "/"
            sub_sub_folder = s3client.list_objects(Bucket = geos_bucket_name, Prefix = prefix_3, Delimiter = '/')
            
            for k in sub_sub_folder.get('CommonPrefixes'):
                sub_sub_path = k.get('Prefix').split('/')
                sub_sub_path = sub_sub_path[:-1]
                goes18_data_dict['ID'].append(id)
                goes18_data_dict['Product_Name'].append(sub_sub_path[0])
                goes18_data_dict['Year'].append(sub_sub_path[1])
                goes18_data_dict['Day'].append(sub_sub_path[2])
                goes18_data_dict['Hour'].append(sub_sub_path[3])
                id += 1
    
    goes18_data = pd.DataFrame(goes18_data_dict)
    return goes18_data

def main():
    goes18_metadata = goes18_data()

if __name__ == "__main__":
    logging.info("GOES18 Data Scraping Starts")
    main()
    logging.info("GOES18 Data Scraping Ends")