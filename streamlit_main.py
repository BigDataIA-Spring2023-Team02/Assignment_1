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
goes_sqlite = GoesSqlite()
nexrad_sqlite = NexradSqlite()
geos_data = goes_sqlite.main()
nexrad_data = nexrad_sqlite.main()

def geos_search_field(satellite_input):
    st.markdown("<h3 style='text-align: center;'>Search By Field</h1>", unsafe_allow_html=True)
    
    st.write(geos_data)

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

                    if st.button('Download to S3 Bucket'):
                        selected_file = product_input + '/' + year_input + '/' + day_input + '/' + hour_input + '/' + file_input
                        url_s3, url_noaa = aws_main.copy_file_to_user_bucket(selected_file, file_input, satellite_input)
                        # st.write('File Link in S3 Bucket !!!\n', url_s3)
                        # st.write('File Link in NOAA Bucket !!!\n', url_noaa)

def geos_search_filename(satellite_input):
    st.markdown("<h3 style='text-align: center;'>Search By Filename</h1>", unsafe_allow_html=True)
    file_name = st.text_input('NOAA GEOS-18 Filename',)
    
    try:
        if st.button('Download to S3 Bucket'):
            url, selected_file_key = goes_18_link_generation(file_name)
            url_s3, url_noaa = aws_main.copy_file_to_user_bucket(selected_file_key, file_name, satellite_input)
            st.write('File Link in S3 Bucket !!!\n', url_s3)
            st.write('File Link in NOAA Bucket !!!\n', url_noaa)
        
    except ValueError:
        st.error('Oops! Unable to Generate')

def nexrad_search_field(satellite_input):
    
    st.markdown("<h3 style='text-align: center;'>Search By Field</h1>", unsafe_allow_html=True)
    
    year_input = st.selectbox('Select Year', nexrad_data['Year'].unique())
    if year_input:
        months = nexrad_data[nexrad_data['Year'] == year_input]['Month'].unique()
        month_input = st.selectbox('Select Month', months)
        if month_input:
            day = nexrad_data[nexrad_data['Month'] == month_input]['Day'].unique()
            day_input = st.selectbox('Select Day', day)
            if day_input:
                station_code = nexrad_data[nexrad_data['Day'] == day_input]['NexRad Station Code'].unique()
                station_code_input = st.selectbox('NexRad Station Code', station_code)
                if station_code_input:
                    files_list = aws_main.list_files_in_noaa_nexrad_bucket(year_input, month_input, day_input, station_code_input)
                    file_input = st.selectbox('Select File Name', files_list)

                    if st.button('Download to S3 Bucket'):
                        selected_file = year_input + '/' + month_input + '/' + day_input + '/' + station_code_input + '/' + file_input
                        url_s3, url_noaa = aws_main.copy_file_to_user_bucket(selected_file, file_input, satellite_input)
                        
def nexrad_search_filename(satellite_input):
    st.markdown("<h3 style='text-align: center;'>Search By Filename</h1>", unsafe_allow_html=True)
    file_name = st.text_input('NOAA GEOS-18 Filename',)
    
    try:
        if st.button('Download to S3 Bucket'):
            url, selected_file_key = nexrad_link_generation(file_name)
            url_s3, url_noaa = aws_main.copy_file_to_user_bucket(selected_file_key, file_name, satellite_input)
            
    except ValueError:
        st.error('Oops! Unable to Generate')

def geos_dataset():
    option = st.selectbox('Select the option to search file', ('--Select Search Type--', 'Search By Field', 'Search By Filename'))
    satellite_input = 'geos18'

    if option == '--Select Search Type--':
        st.error('Select an input field')
    elif option == 'Search By Field':
        geos_search_field(satellite_input)
    
    elif option == 'Search By Filename':
        geos_search_filename(satellite_input)

def nexrad_dataset():
    option = st.selectbox('Select the option to search file', ('--Select Search Type--', 'Search By Field', 'Search By Filename'))
    satellite_input = 'nexrad'

    if option == '--Select Search Type--':
        st.error('Select an input field')
    elif option == 'Search By Field':
        nexrad_search_field(satellite_input)
    
    elif option == 'Search By Filename':
        nexrad_search_filename(satellite_input)

def nexrad_mapdata():

    return

def main():
    page = st.sidebar.selectbox("Choose a page", ["--Select Data--", "GEOS Data", "NexRad Data", "NexRad Map Locations"])
    st.markdown("<h1 style='text-align: center;'>Geospatial Data Exploration Tool</h1>", unsafe_allow_html=True)

    if page == "--Select Data--":
        st.markdown("<h3 style='text-align: center;'>Select a page on the left</h1>", unsafe_allow_html=True)
    
    elif page == "GEOS Data":
        st.markdown("<h2 style='text-align: center;'>Data Exploration of the GEOS dataset</h1>", unsafe_allow_html=True)
        geos_dataset()

    elif page == "NexRad Data":
        st.markdown("<h2 style='text-align: center;'>Data Exploration of the NexRad dataset</h1>", unsafe_allow_html=True)
        nexrad_dataset()
    
    elif page == "NexRad Map Locations":
        st.markdown("<h2 style='text-align: center;'>Data Exploration of the NexRad dataset</h1>", unsafe_allow_html=True)
        nexrad_mapdata()
    
    return

if __name__ == "__main__":
    main()