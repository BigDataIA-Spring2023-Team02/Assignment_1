import os
import logging
import streamlit as st
from file_url_generate import *
from aws_main import *
from noaa_scrape_data import *

LOGLEVEL = os.environ.get('LOGLEVEL', 'INFO').upper()
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=LOGLEVEL,
    datefmt='%Y-%m-%d %H:%M:%S',
    filename='logs.log')

def geos_dataset():
    option = st.selectbox('Select the option to search file', ('--Select Search Type--', 'Search By Field', 'Search By Filename'))
    
    try:
        if option == '--Select Search Type--':
            st.error('Select an input field')
        elif option == 'Search By Field':
            st.markdown("<h3 style='text-align: center;'>Search By Field</h1>", unsafe_allow_html=True)
            
            product_input = st.selectbox('Select the Product Name', ['ABI-L1b-RadC'])
            year_input = st.selectbox('Year', range(2020, 2023))
            day_input = st.selectbox('Day', range(1, 365))
            hour_input = st.selectbox('Hour', range(00, 24))
            file_input = st.selectbox('Select the File Name', [''])
            
            goes18_filename_link_generation(product_input, year_input, day_input, hour_input, file_input)
        elif option == 'Search By Filename':
            st.markdown("<h3 style='text-align: center;'>Search By Filename</h1>", unsafe_allow_html=True)
            file_name = st.text_input('NOAA GEOS-18 Filename',)
            
            try:
                if st.button('Generate URL Link'):
                    url = goes_18_link_generation(file_name)
                    st.write('Generated URL Link:\n', url)
            except ValueError:
                st.error('Oops! Unable to Generate')

    except ValueError:
        st.error('Oops! Did not find any input')

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
    
    return

if __name__ == "__main__":
    main()