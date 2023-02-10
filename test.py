from file_url_generate import goes_18_link_generation, nexrad_link_generation
import pandas as pd

goes_url = "https://docs.google.com/spreadsheets/d/1o1CLsm5OR0gH5GHbTsPWAEOGpdqqS49-P5e14ugK37Q/export?format=csv"
goes_df = pd.read_csv(goes_url)
goes_df = goes_df[['File name','Full file name']]

nexrad_url = "https://docs.google.com/spreadsheets/d/1o1CLsm5OR0gH5GHbTsPWAEOGpdqqS49-P5e14ugK37Q/export?format=csv&gid=651299232"
nexrad_df = pd.read_csv(nexrad_url)
nexrad_df = nexrad_df[['File name','Full file name']]
# print(goes_18_link_generation(goes_df['File name'][7]))
# print(goes_df['File name'][7])

def test_case01():
    goes_generated_url, goes_selected_file_key = goes_18_link_generation(goes_df['File name'][1])
    nexrad_generated_url, nexrad_selected_file_key = nexrad_link_generation(nexrad_df['File name'][1])
    goes_url = goes_df['Full file name'][1]
    nexrad_url = nexrad_df['Full file name'][1]
    assert goes_generated_url == goes_url
    assert nexrad_generated_url == nexrad_url

def test_case02():
    goes_generated_url, goes_selected_file_key = goes_18_link_generation(goes_df['File name'][2])
    nexrad_generated_url, nexrad_selected_file_key = nexrad_link_generation(nexrad_df['File name'][2])
    goes_url = goes_df['Full file name'][2]
    nexrad_url = nexrad_df['Full file name'][2]
    assert goes_generated_url == goes_url
    assert nexrad_generated_url == nexrad_url

def test_case03():
    goes_generated_url, goes_selected_file_key = goes_18_link_generation(goes_df['File name'][3])
    nexrad_generated_url, nexrad_selected_file_key = nexrad_link_generation(nexrad_df['File name'][3])
    goes_url = goes_df['Full file name'][3]
    nexrad_url = nexrad_df['Full file name'][3]
    assert goes_generated_url == goes_url
    assert nexrad_generated_url == nexrad_url

def test_case04():
    goes_generated_url, goes_selected_file_key = goes_18_link_generation(goes_df['File name'][4])
    nexrad_generated_url, nexrad_selected_file_key = nexrad_link_generation(nexrad_df['File name'][4])
    goes_url = goes_df['Full file name'][4]
    nexrad_url = nexrad_df['Full file name'][4]
    assert goes_generated_url == goes_url
    assert nexrad_generated_url == nexrad_url

def test_case05():
    goes_generated_url, goes_selected_file_key = goes_18_link_generation(goes_df['File name'][5])
    nexrad_generated_url, nexrad_selected_file_key = nexrad_link_generation(nexrad_df['File name'][5])
    goes_url = goes_df['Full file name'][5]
    nexrad_url = nexrad_df['Full file name'][5]
    assert goes_generated_url == goes_url
    assert nexrad_generated_url == nexrad_url

def test_case06():
    goes_generated_url, goes_selected_file_key = goes_18_link_generation(goes_df['File name'][6])
    nexrad_generated_url, nexrad_selected_file_key = nexrad_link_generation(nexrad_df['File name'][6])
    goes_url = goes_df['Full file name'][6]
    nexrad_url = nexrad_df['Full file name'][6]
    assert goes_generated_url == goes_url
    assert nexrad_generated_url == nexrad_url

def test_case07():
    goes_generated_url, goes_selected_file_key = goes_18_link_generation(goes_df['File name'][7])
    nexrad_generated_url, nexrad_selected_file_key = nexrad_link_generation(nexrad_df['File name'][7])
    goes_url = goes_df['Full file name'][7]
    nexrad_url = nexrad_df['Full file name'][7]
    assert goes_generated_url == goes_url
    assert nexrad_generated_url == nexrad_url

def test_case08():
    goes_generated_url, goes_selected_file_key = goes_18_link_generation(goes_df['File name'][8])
    nexrad_generated_url, nexrad_selected_file_key = nexrad_link_generation(nexrad_df['File name'][8])
    goes_url = goes_df['Full file name'][8]
    nexrad_url = nexrad_df['Full file name'][8]
    assert goes_generated_url == goes_url
    assert nexrad_generated_url == nexrad_url

def test_case09():
    goes_generated_url, goes_selected_file_key = goes_18_link_generation(goes_df['File name'][9])
    nexrad_generated_url, nexrad_selected_file_key = nexrad_link_generation(nexrad_df['File name'][9])
    goes_url = goes_df['Full file name'][9]
    nexrad_url = nexrad_df['Full file name'][9]
    assert goes_generated_url == goes_url
    assert nexrad_generated_url == nexrad_url

def test_case10():
    goes_generated_url, goes_selected_file_key = goes_18_link_generation(goes_df['File name'][10])
    nexrad_generated_url, nexrad_selected_file_key = nexrad_link_generation(nexrad_df['File name'][10])
    goes_url = goes_df['Full file name'][10]
    nexrad_url = nexrad_df['Full file name'][10]
    assert goes_generated_url == goes_url
    assert nexrad_generated_url == nexrad_url

def test_case11():
    goes_generated_url, goes_selected_file_key = goes_18_link_generation(goes_df['File name'][11])
    nexrad_generated_url, nexrad_selected_file_key = nexrad_link_generation(nexrad_df['File name'][11])
    goes_url = goes_df['Full file name'][11]
    nexrad_url = nexrad_df['Full file name'][11]
    assert goes_generated_url == goes_url
    assert nexrad_generated_url == nexrad_url

def test_case12():
    goes_generated_url, goes_selected_file_key = goes_18_link_generation(goes_df['File name'][12])
    nexrad_generated_url, nexrad_selected_file_key = nexrad_link_generation(nexrad_df['File name'][12])
    goes_url = goes_df['Full file name'][12]
    nexrad_url = nexrad_df['Full file name'][12]
    assert goes_generated_url == goes_url
    assert nexrad_generated_url == nexrad_url