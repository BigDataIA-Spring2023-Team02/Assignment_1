import re
import requests
from dotenv import load_dotenv

def goes_18_link_generation(file):
    try:
        start_url = "https://noaa-goes18.s3.amazonaws.com/"
        file = file.strip()

        if (re.match(r'[O][R][_][A-Z]{3}[-][A-Za-z0-9]{2,3}[-][A-Za-z0-9]{4,6}[-][A-Z0-9]{2,5}[_][G][1][8][_][s][0-9]{14}[_][e][0-9]{14}[_][c][0-9]{14}\b', file)):
            file_name = file.split("_")
            c = 0
            z = '-'
            str1 = []
            for y in file_name[1]:
                if y == z:
                    c += 1
                if c == 2:
                    str1.append(y)
            str_2 = ''.join(str1[1:-1])
            year = file_name[3][1:5]
            day = file_name[3][5:8]
            hour = file_name[3][8:10]
            url = start_url + file_name[1][0:7] + str_2 + '/' + year + '/' + day + '/' + hour + '/' + file
            
            response = requests.get(url)
            if(response.status_code == 404):
                print("Sorry! No such file exists.")
                raise SystemExit()
            return url
        
        else:
            print("Invalid Filename format, please follow format for GOES18!")
            raise SystemExit()

    except:
        print("No input Filename.")

def nexrad_link_generation(file):
    try:
        start_url = "https://noaa-nexrad-level2.s3.amazonaws.com/"
        file = file.strip()

        if (re.match(r'[A-Z]{3}[A-Z0-9][0-9]{8}[_][0-9]{6}[_]{0,1}[A-Z]{0,1}[0-9]{0,2}[_]{0,1}[A-Z]{0,3}\b', file)):
            url = start_url + file[4:8] + "/" + file[8:10] + "/" + file[10:12] + "/" + file[:4] + "/" + file
            response = requests.get(url)
            if(response.status_code == 404):
                print("Sorry! No such file exists.")
                raise SystemExit()
            return url
        
        else:
            print("Invalid Filename format, please follow format for GOES18!")
            raise SystemExit()

    except:
        print("No input Filename.")

def goes18_filename_link_generation(product_name, year_input, day_input, hour_input, file_input):
    try:
        start_url = "https://noaa-goes18.s3.amazonaws.com/"
        product = product_name
        year = year_input
        day = day_input
        hour = hour_input
        file = file_input
        url = start_url + '/' + product + '/' + year + '/' + day + '/' + hour + '/' + file
        
        response = requests.get(url)
        if(response.status_code == 404):
            print("Sorry! No such file exists.")
            raise SystemExit()
        return url
        
    except:
        print("No input Filename.")

def nexrad_filename_link_generation(product_name, year_input, day_input, hour_input, file_input):
    try:
        start_url = "https://noaa-goes18.s3.amazonaws.com/"
        product = product_name
        year = year_input
        day = day_input
        hour = hour_input
        file = file_input
        url = start_url + '/' + product + '/' + year + '/' + day + '/' + hour + '/' + file
        
        response = requests.get(url)
        if(response.status_code == 404):
            print("Sorry! No such file exists.")
            raise SystemExit()
        return url
        
    except:
        print("No input Filename.")
