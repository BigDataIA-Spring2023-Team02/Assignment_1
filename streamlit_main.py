import os
import logging
import streamlit as st
import boto3
from file_url_generate import *
from aws_main import *
from noaa_scrape_data import *
import pandas as pd


from sqlite import *


LOGLEVEL = os.environ.get('LOGLEVEL', 'INFO').upper()
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=LOGLEVEL,
    datefmt='%Y-%m-%d %H:%M:%S',
    filename='logs.log')

aws_main = AWS_Main()
noaa_scrape = Scrape_Data()

geos_data = noaa_scrape.geos18_data()

goessqlite = GoesSqlite()
geos_data = goessqlite.main()
nexradsqlite = NexradSqlite()
nexrad_data = nexradsqlite.main()
df = pd.DataFrame(columns=['col1'])

# write the initial dataframe to the CSVUs file
df.to_csv("/Users/siddhisawant/Downloads/BigDataSys/GitHub/Assignment_1/meet.csv", index=False)





def geos_dataset():
    option = st.selectbox('Select the option to search file', ('--Select Search Type--', 'Search By Field', 'Search By Filename'))

    if option == '--Select Search Type--':
        st.error('Select an input field')
    elif option == 'Search By Field':
        st.markdown("<h3 style='text-align: center;'>Search By Field</h1>", unsafe_allow_html=True)
        
        product_input = st.selectbox('Select Product Name', geos_data['Product_Name'].unique())
        if product_input:
            years = geos_data[geos_data['Product_Name'] == product_input]['Year'].unique()
            year_input = st.selectbox('Select Year', years)
            if year_input:
                day = geos_data[geos_data['Year'] == year_input]['Day'].unique()
                day_input = st.selectbox('Select Day', day)
                if day_input:
                    hour = geos_data[geos_data['Day'] == day_input]['Hour'].unique()
                    hour_input = st.selectbox('Select Hour', hour)
                    if hour_input:
                        files_list = aws_main.list_files_in_noaa_goes18_bucket(product_input, year_input, day_input, hour_input)
                        file_input = st.selectbox('Select File Name', files_list)

                        if st.button('Generate URL Link'):
                            url = goes18_filename_link_generation(product_input, year_input, day_input, hour_input, file_input)
                            
                            bucket_name = "damg-7245-projects"
                            file_name = "/Users/ajinabraham/Documents/GitHub/BigData/Assignment_1/meet.csv"

                            client = boto3.client('s3',region_name = 'us-east-1',aws_access_key_id = os.environ.get('AWS_ACCESS_KEY'),aws_secret_access_key = os.environ.get('AWS_SECRET_KEY'))

                            #client.download_file(bucket_name, file_name, "meet.txt") 
                            #with open("/Users/ajinabraham/Documents/GitHub/BigData/Assignment_1/meet.xlsx","a") as file:file.write(url)
                           # aws_main.list_files_in_user_bucket()
                            st.write('Generated URL Link:\n', url)
                        
                        if st.button('Download to S3 Bucket'):
                            st.write("Inside Download Button")
                            # upload_file_to_user_bucket()
                            # url_3, url_noaa = upload_file_to_user_bucket()
                            # st.write('File Link in S3 Bucket !!!\n', url_s3)
                            # st.write('File Link in NOAA Bucket !!!\n', url_noaa)

    elif option == 'Search By Filename':
        st.markdown("<h3 style='text-align: center;'>Search By Filename</h1>", unsafe_allow_html=True)
        file_name = st.text_input('NOAA GEOS-18 Filename',)
        
        try:
            if st.button('Generate URL Link'):
                url = goes_18_link_generation(file_name)
                st.write('Generated URL Link:\n', url)
        except ValueError:
            st.error('Oops! Unable to Generate')


def nexrad_dataset():
    option = st.selectbox('Select the option to search file', ('--Select Search Type--', 'Search By Field', 'Search By Filename'))

    if option == '--Select Search Type--':
        st.error('Select an input field')
    elif option == 'Search By Field':
        st.markdown("<h3 style='text-align: center;'>Search By Field</h1>", unsafe_allow_html=True)
        
        product_input = st.selectbox('Select Year', nexrad_data['Year'].unique())
        if product_input:
            month = nexrad_data[nexrad_data['Year'] == product_input]['Month'].unique()
            month_input = st.selectbox('Select Month', month)
            if month_input:
                day = nexrad_data[nexrad_data['Month'] == month_input]['Day'].unique()
                day_input = st.selectbox('Select Day', day)
                if day_input:
                    station = nexrad_data[nexrad_data['Day'] == day_input]['NexRad Station Code'].unique()
                    station_input = st.selectbox('Select NexRad Station Code', station)
                    if station_input:
                        files_list = aws_main.list_files_in_noaa_nexrad_bucket(product_input, month_input, day_input, station_input)
                        file_input = st.selectbox('Select File Name', files_list)

                        if st.button('Generate URL Link'):
                            url = nexrad_filename_link_generation(product_input, month_input, day_input, file_input, station_input)
                            
                            bucket_name = "damg-7245-projects"
                            file_name = "/Users/ajinabraham/Documents/GitHub/BigData/Assignment_1/meet.csv"

                            client = boto3.client('s3',region_name = 'us-east-1',aws_access_key_id = os.environ.get('AWS_ACCESS_KEY'),aws_secret_access_key = os.environ.get('AWS_SECRET_KEY'))

                                #client.download_file(bucket_name, file_name, "meet.txt") 
                                #with open("/Users/ajinabraham/Documents/GitHub/BigData/Assignment_1/meet.xlsx","a") as file:file.write(url)
                            # aws_main.list_files_in_user_bucket()
                        if st.button('Download to S3 Bucket'):
                            st.write("Inside Download Button")
                            # upload_file_to_user_bucket()
                            # url_3, url_noaa = upload_file_to_user_bucket()
                            # st.write('File Link in S3 Bucket !!!\n', url_s3)
                            # st.write('File Link in NOAA Bucket !!!\n', url_noaa)

    elif option == 'Search By Filename':
        st.markdown("<h3 style='text-align: center;'>Search By Filename</h1>", unsafe_allow_html=True)
        file_name = st.text_input('NEXRAD Filename',)
        
        try:
            if st.button('Generate URL Link'):
                url = noaa_scrape.nexrad_link_generatio(file_name)
                st.write('Generated URL Link:\n', url)
        except ValueError:
            st.error('Oops! Unable to Generate')



def main():
    page = st.sidebar.selectbox("Choose a page", ["--Select Data--", "GEOS Data", "NexRad Data"])
    st.markdown("<h1 style='text-align: center;'>Geospatial Data Exploration Tool</h1>", unsafe_allow_html=True)

    if page == "--Select Data--":
        st.markdown("<h3 style='text-align: center;'>Select a page on the left</h1>", unsafe_allow_html=True)
    
    elif page == "GEOS Data":
        st.markdown("<h2 style='text-align: center;'>Data Exploration of the GEOS dataset</h1>", unsafe_allow_html=True)
        geos_dataset()

    elif page == "NexRad Data":
        st.markdown("<h2 style='text-align: center;'>Data Exploration of the NexRad dataset</h1>", unsafe_allow_html=True)
        nexrad_dataset()
    
    return

if __name__ == "__main__":
    main()
