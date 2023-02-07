import os
import boto3
import logging
import pandas as pd

class Scrape_Data:
    def __init__(self):

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
        geos18_data_dict = {'ID': [], 'Product_Name': [], 'Year': [], 'Day': [], 'Hour': []}

        nexrad_bucket_name = "noaa-nexrad-level2"
        nexrad_data_dict = {'ID': [], 'Year': [], 'Month': [], 'Day': [], 'NexRad Station Code': []}

        def geos18_data():
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
                        geos18_data_dict['ID'].append(id)
                        geos18_data_dict['Product_Name'].append(sub_sub_path[0])
                        geos18_data_dict['Year'].append(sub_sub_path[1])
                        geos18_data_dict['Day'].append(sub_sub_path[2])
                        geos18_data_dict['Hour'].append(sub_sub_path[3])
                        id += 1
            
            geos18_data = pd.DataFrame(geos18_data_dict)
            return geos18_data

        def nexrad_data():
            id = 1
            years = ['2022','2023']
            for year in years:
                prefix = year + '/'
                result = s3client.list_objects(Bucket = nexrad_bucket_name, Prefix = prefix, Delimiter = '/')
                
                for i in result.get('CommonPrefixes'):
                    path = i.get('Prefix').split('/')
                    prefix_2 = prefix + path[-2] + "/"
                    sub_folder = s3client.list_objects(Bucket = nexrad_bucket_name, Prefix = prefix_2, Delimiter = '/')
                    
                    for j in sub_folder.get('CommonPrefixes'):
                        sub_path = j.get('Prefix').split('/')
                        prefix_3 = prefix_2 + sub_path[-2] + "/"
                        sub_sub_folder = s3client.list_objects(Bucket = nexrad_bucket_name, Prefix = prefix_3, Delimiter = '/')

                        for k in sub_sub_folder.get('CommonPrefixes'):
                            sub_sub_path = k.get('Prefix').split('/')
                            sub_sub_path = sub_sub_path[:-1]
                            nexrad_data_dict['ID'].append(id)
                            nexrad_data_dict['Year'].append(sub_sub_path[0])
                            nexrad_data_dict['Month'].append(sub_sub_path[1])
                            nexrad_data_dict['Day'].append(sub_sub_path[2])
                            nexrad_data_dict['NexRad Station Code'].append(sub_sub_path[3])
                            id += 1
            
            nexrad_data = pd.DataFrame(nexrad_data_dict)
            return nexrad_data