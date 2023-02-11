import os
import logging
import streamlit as st
import boto3
from file_url_generate import *
from aws_main import *
from noaa_scrape_data import *
import pandas as pd
from sqlite import *
import geopandas as gpd
import matplotlib
import matplotlib.pyplot as plt 
import folium
from streamlit_folium import st_folium
from streamlit_folium import folium_static
from PIL import Image
import base64
import time

load_dotenv()

clientLogs = boto3.client('logs',
                        region_name='us-east-1',
                        aws_access_key_id = os.environ.get('AWS_LOGS_ACCESS_KEY'),
                        aws_secret_access_key = os.environ.get('AWS_LOGS_SECRET_KEY')
                        )

def write_logs(message: str):
    clientLogs.put_log_events(
        logGroupName = "Assignment01-logs",
        logStreamName = "Test-Logs",
        logEvents = [
            {
                'timestamp' : int(time.time() * 1e3),
                'message' : message
            }
        ]
    )

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
nexradmap_sqlite = NexradMapSqlite()
geos_data = goes_sqlite.main()
nexrad_data = nexrad_sqlite.main()
nexradmap_data = nexradmap_sqlite.main()

def geos_search_field(satellite_input):
    st.markdown("<h3 style='text-align: center;'>Search Through Fields 🔎</h1>", unsafe_allow_html=True)
    write_logs(f"Into GEOS Data by Search Field Inputs")
    
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
                    write_logs(f"Selected file to copy to user bucket {file_input}")
                    
                    if st.button('Copy to User S3 Bucket ©️'):
                        selected_file = product_input + '/' + year_input + '/' + day_input + '/' + hour_input + '/' + file_input
                        write_logs(f"Selected file key: {selected_file}")
                        url_s3, url_noaa = aws_main.copy_file_to_user_bucket(selected_file, file_input, satellite_input)
                        write_logs(f"File URL in User S3 Bucket {url_s3}")
                        write_logs(f"File URL in NOAA S3 Bucket {url_noaa}")

def geos_search_filename(satellite_input):
    st.markdown("<h3 style='text-align: center;'>Search Through Filename 🔎</h1>", unsafe_allow_html=True)
    logging.info('Into GEOS Data by Search Filename Inputs')
    file_name = st.text_input('NOAA GEOS-18 Filename',)

    try:
        if st.button('Copy to User S3 Bucket ©️'):
            url, selected_file_key = goes_18_link_generation(file_name)
            url_s3, url_noaa = aws_main.copy_file_to_user_bucket(selected_file_key, file_name, satellite_input)
            write_logs(f"File URL in User S3 Bucket {url_s3}")
            write_logs(f"File URL in NOAA S3 Bucket {url_noaa}")
            
    except ValueError:
        write_logs(f"Not able to generate filename URL")
        st.error('Oops! Unable to Generate')

def nexrad_search_field(satellite_input):
    st.markdown("<h3 style='text-align: center;'>Search Through Field 🔎</h1>", unsafe_allow_html=True)
    logging.info('Into NEXRAD Data by Search Field Inputs')

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
                    file_input = st.selectbox('Select File Name',files_list)
                    write_logs(f"Selected file to copy to user bucket {file_input}")

                    if st.button('Copy to User S3 Bucket ©️'):
                        selected_file = year_input + '/' + month_input + '/' + day_input + '/' + station_code_input + '/' + file_input
                        write_logs(f"Selected file key: {selected_file}")
                        url_s3, url_noaa = aws_main.copy_file_to_user_bucket(selected_file, file_input, satellite_input)
                        write_logs(f"File URL in User S3 Bucket {url_s3}")
                        write_logs(f"File URL in NOAA S3 Bucket {url_noaa}")

def nexrad_search_filename(satellite_input):
    st.markdown("<h3 style='text-align: center;'>Search Through Filename 🔎</h1>", unsafe_allow_html=True)
    logging.info('Into NEXRAD Data by Search Filename Inputs')
    file_name = st.text_input('NOAA GEOS-18 Filename',)
    
    try:
        if st.button('Copy to User S3 Bucket ©️'):
            url, selected_file_key = nexrad_link_generation(file_name)
            url_s3, url_noaa = aws_main.copy_file_to_user_bucket(selected_file_key, file_name, satellite_input)
            write_logs(f"File URL in User S3 Bucket {url_s3}")
            write_logs(f"File URL in NOAA S3 Bucket {url_noaa}")
    
    except ValueError:
        write_logs(f"Not able to generate filename URL")
        st.error('Oops! Unable to Generate')

def geos_dataset():
    option = st.selectbox('Select the option to search file', ('--Select Search Type--', 'Search By Field 🔎', 'Search By Filename 🔎'))
    satellite_input = 'geos18'
    
    if option == '--Select Search Type--':
        st.error('Select an input field')
    elif option == 'Search By Field 🔎':
        geos_search_field(satellite_input)
    
    elif option == 'Search By Filename 🔎':
        geos_search_filename(satellite_input)

def nexrad_dataset():
    option = st.selectbox('Select the option to search file', ('--Select Search Type--', 'Search By Field 🔎', 'Search By Filename 🔎'))
    satellite_input = 'nexrad'

    if option == '--Select Search Type--':
        st.error('Select an input field')
    elif option == 'Search By Field 🔎':
        nexrad_search_field(satellite_input)
    
    elif option == 'Search By Filename 🔎':
        nexrad_search_filename(satellite_input)

def nexrad_mapdata(nexradusweather_data):
    write_logs(f"Running NEXRAD Map Data Exploration")
    st.map(nexradmap_data)
    
    m = folium.Map(location=[20,0], tiles="OpenStreetMap", zoom_start=2)
    for i in range(0,len(nexradmap_data)):
        folium.Marker(
        location = [nexradmap_data.iloc[i]['LAT'], nexradmap_data.iloc[i]['LON']],
        popup = (nexradmap_data.iloc[i]['ICAO'],nexradmap_data.iloc[i]['NAME'])
        ).add_to(m)

    st.markdown("<h2 style='text-align: center;'>Nexrad Station Pointers</h1>", unsafe_allow_html=True)
    folium_static(m)

    path = "tl_2022_us_state/tl_2022_us_state.shp"
    df = gpd.read_file(path)
    df.crs = "epsg:4326"
    df2 = df.iloc[[35,36],:] 
    df3 = df.iloc[[40,41],:]
    df.drop([35,36,41,40], axis=0, inplace=True)

    geo_data = gpd.GeoDataFrame(nexradusweather_data, geometry = gpd.points_from_xy(nexradusweather_data.LONGITUDE,nexradusweather_data.LATITUDE))
    geo_data2 = geo_data.iloc[[148],:]
    geo_data3 = geo_data.iloc[[142,143,144,145,146,147,155],:]
    geo_data.drop([148,142,143,144,145,146,147,155], axis=0, inplace=True)

    st.markdown("<h3 style='text-align: center;'>Nexrad Locations in Mainland USA</h3>", unsafe_allow_html=True)
    axis = df.plot(cmap='magma')
    geo_data.plot(ax = axis, color = 'lightgreen')
    figure1 = plt.gcf()
    st.pyplot(figure1)

    st.markdown("<h3 style='text-align: center;'>Nexrad Locations in Guam</h3>", unsafe_allow_html=True)
    axis = df2.plot(cmap='magma')
    geo_data2.plot(ax = axis, color = 'lightgreen')
    figure2 = plt.gcf()
    st.pyplot(figure2)

    st.markdown("<h3 style='text-align: center;'>Nexrad Locations in Alaska</h3>", unsafe_allow_html=True)
    axis = df3.plot(cmap = 'magma')
    geo_data3.plot(ax = axis, color = 'lightgreen')
    figure3 = plt.gcf()
    st.pyplot(figure3)

    return

def main():
    page = st.sidebar.selectbox("Choose a page", ["--Select Data--", "GEOS Data 🌎", "NexRad Data 🌎", "NexRad Map Locations 📍"])
    for i in range(15):
        st.sidebar.write('')

    st.sidebar.image("Images/Earth-Free-Download-PNG.png", width = 200)
    st.markdown("<h1 style='text-align: center;'>Geospatial Data Exploration Tool 🔭</h1>", unsafe_allow_html=True)
    
    if page == "--Select Data--":
        st.markdown("<h3 style='text-align: center;'>Select page from the Left Selectbox</h1>", unsafe_allow_html=True)
        st.write('')
        st.image(Image.open('Images/DataExploration.png'))
    
    elif page == "GEOS Data 🌎":
        st.markdown("<h2 style='text-align: center;'>Data Exploration of the GEOS dataset 🌎</h1>", unsafe_allow_html=True)
        st.write('')
        st.image(Image.open('Images/Satellite-data-for-imagery.jpeg'))
        write_logs(f"Running GEOS dataset file download script")
        geos_dataset()

    elif page == "NexRad Data 🌎":
        st.markdown("<h2 style='text-align: center;'>Data Exploration of the NexRad dataset 🌎</h1>", unsafe_allow_html=True)
        st.write('')
        st.image(Image.open('Images/SatelliteImage.jpeg'))
        write_logs(f"Running NEXRAD dataset file download script")
        nexrad_dataset()
    
    elif page == "NexRad Map Locations 📍":
        st.markdown("<h2 style='text-align: center;'>NexRad Map Geo-Locations 📍</h1>", unsafe_allow_html=True)
        nexradusweather_data = pd.read_csv("Data/Weather_Radar_Stations.csv")
        nexradusweather_data.rename(columns = {'Y':'LATITUDE', 'X':'LONGITUDE'}, inplace = True)
        write_logs(f"Running NEXRAD Map Geo-location script")
        nexrad_mapdata(nexradusweather_data)
    
    return

if __name__ == "__main__":
    write_logs(f"Streamlit app script starts")
    main()
    write_logs(f"Streamlit app script ends")