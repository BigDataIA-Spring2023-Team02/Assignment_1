import os
import boto3
import logging
from dotenv import load_dotenv
import streamlit as st

class AWS_Main:
    def __init__(self):
        load_dotenv ()

        LOGLEVEL = os.environ.get('LOGLEVEL', 'INFO').upper()
        logging.basicConfig(
            format='%(asctime)s %(levelname)-8s %(message)s',
            level=LOGLEVEL,
            datefmt='%Y-%m-%d %H:%M:%S',
            filename='logs.log')

        self.s3client = boto3.client('s3',
                                region_name = 'us-east-1',
                                aws_access_key_id = os.environ.get('AWS_ACCESS_KEY'),
                                aws_secret_access_key = os.environ.get('AWS_SECRET_KEY')
                                )
        
        self.s3resource = boto3.resource('s3',
                                region_name = 'us-east-1',
                                aws_access_key_id = os.environ.get('AWS_ACCESS_KEY'),
                                aws_secret_access_key = os.environ.get('AWS_SECRET_KEY')
                                )
        
        self.geos_bucket_name = "noaa-goes18"
        self.nexrad_bucket_name = "noaa-nexrad-level2"
    
    def list_files_in_user_bucket(self):
        my_bucket = self.s3client.list_objects_v2(Bucket = os.environ.get('USER_BUCKET_NAME')).get('Contents')
        file_list = []
        for file in my_bucket:
            file_list.append(file['Key'])
        return file_list

    def list_files_in_noaa_goes18_bucket(self, product, year, day, hour):
        logging.info('Running script to fetch GOES18 filenames for selected fields')
        year_input = year
        day_input = day
        hour_input = hour
        product_name = product
        prefix = product_name + '/' + year_input + '/' + day_input + '/' + hour_input + '/'
        geos_bucket = self.s3client.list_objects(Bucket = self.geos_bucket_name, Prefix = prefix).get('Contents')
        file_list = []
        for objects in geos_bucket:
            file_path = objects['Key']
            file_path = file_path.split('/')
            file_list.append(file_path[-1])
        logging.info('Returning Files List for selected fields:', prefix)
        return file_list

    def list_files_in_noaa_nexrad_bucket(self, year, month, day, station):
        logging.info('Running script to fetch NEXRAD filenames for selected fields')
        year_input = year
        month_input = month
        day_input = day
        station_code = station
        prefix = year_input + '/' + month_input + '/' + day_input + '/' + station_code + '/'
        nexrad_bucket = self.s3client.list_objects(Bucket = self.nexrad_bucket_name, Prefix = prefix).get('Contents')
        file_list = []
        for objects in nexrad_bucket:
            file_path = objects['Key']
            file_path = file_path.split('/')
            file_list.append(file_path[-1])
        logging.info('Returning Files List for selected fields:', prefix)
        return file_list

    def copy_file_to_user_bucket(self, selected_file, file_input, satellite_input):
        user_bucket = self.s3resource.Bucket(os.environ.get('USER_BUCKET_NAME'))  #define the destination bucket as the user bucker
        
        if satellite_input == 'geos18':
            user_folder = 'GOES18/'
            file_key = user_folder + file_input
            url_s3 = 'https://damg-7245-projects.s3.amazonaws.com/' + file_key
            url_noaa = 'https://noaa-goes18.s3.amazonaws.com/' + selected_file
            copy_source = {
                'Bucket': self.geos_bucket_name,
                'Key': selected_file
                }
        
        elif satellite_input == 'nexrad':
            user_folder = 'NEXRAD/'
            file_key = user_folder + file_input
            url_s3 = 'https://damg-7245-projects.s3.amazonaws.com/' + file_key
            url_noaa = 'https://noaa-nexrad-level2.s3.amazonaws.com/' + selected_file
            copy_source = {
                'Bucket': self.nexrad_bucket_name,
                'Key': selected_file
                }
        
        for file in user_bucket.objects.all():
            if(file.key == file_key):
                st.write('Sorry !!! Cannot copy a file that is already present in the user bucket.')
                st.write('DOWNLOAD the file from the below link using URL to already existing file on local S3 bucket: ')
                st.write('File Link in S3 Bucket !!!\n', url_s3)
                return url_s3, url_noaa

        user_bucket.copy(copy_source, file_key)
        st.write('File Link in S3 Bucket !!!\n', url_s3)
        st.write('File Link in NOAA Bucket !!!\n', url_noaa)
        return url_s3, url_noaa