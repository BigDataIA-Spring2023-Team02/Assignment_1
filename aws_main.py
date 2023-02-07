import os
import boto3
import logging
from dotenv import load_dotenv

class AWS_Main:
    def __init__(self):
    
        load_dotenv ()

        LOGLEVEL = os.environ.get('LOGLEVEL', 'INFO').upper()
        logging.basicConfig(
            format='%(asctime)s %(levelname)-8s %(message)s',
            level=LOGLEVEL,
            datefmt='%Y-%m-%d %H:%M:%S',
            filename='logs.log')

        s3client = boto3.client('s3',
                                region_name = 'us-east-1',
                                aws_access_key_id = os.environ.get('AWS_ACCESS_KEY'),
                                aws_secret_access_key = os.environ.get('AWS_SECRET_KEY')
                                )

        geos_bucket_name = "noaa-goes18"
        nexrad_bucket_name = "noaa-nexrad-level2"

        def list_files_in_user_bucket():
            logging.debug("fetching objects in user s3 bucket")
            my_bucket = s3client.list_objects_v2(Bucket = os.environ.get('USER_BUCKET_NAME')).get('Contents')
            logging.info("Printing files from user S3 bucket")
            file_list = []
            for file in my_bucket:
                file_list.append(file['Key'])
            return file_list

        def list_files_in_noaa_goes18_bucket():
            logging.debug("fetching objects in noaa goes18 s3 bucket")
            # product_name, year_input, day_input, hour_input
            year_input = '2022'
            day_input = '209'
            hour_input = '00'
            product_name = 'ABI-L1b-RadC'
            prefix = product_name + '/' + year_input + '/' + day_input + '/' + hour_input + '/'
            geos_bucket = s3client.list_objects(Bucket = geos_bucket_name, Prefix = prefix).get('Contents')
            logging.info("Printing files from GOES18 S3 bucket")
            print("Files available to download from the selected location:")
            file_list = []
            for objects in geos_bucket:
                file_path = objects['Key']
                file_path = file_path.split('/')
                file_list.append(file_path[-1])
            return file_list

        def list_files_in_noaa_nexrad_bucket():
            logging.debug("fetching objects in noaa nexrad s3 bucket")
            # year_input, month_input, day_input, station_code
            year_input = '2022'
            month_input = '02'
            day_input = '05'
            station_code = 'KABX'
            prefix = year_input + '/' + month_input + '/' + day_input + '/' + station_code + '/'
            geos_bucket = s3client.list_objects(Bucket = nexrad_bucket_name, Prefix = prefix).get('Contents')
            logging.info("Printing files from NEXRAD bucket")
            print("Files available to download from the selected location:")
            file_list = []
            for objects in geos_bucket:
                file_path = objects['Key']
                file_path = file_path.split('/')
                file_list.append(file_path[-1])
            return file_list