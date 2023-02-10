# Big Data Systems & Intelligence Analytics

## Team Information
| Name     | NUID        |
| ---      | ---         |
| Meet     | 002776055   |
| Ajin     | 002745287   |
| Siddhi   | 002737346   |
| Akhil    | 002766590   |

## About
This repository contains a collection of Big Data Systems & Intelligence Analytics Assignments & Projects that utilize the power of AWS and SQLite to process and analyze data using Streamlit. The assignments are designed to showcase the capabilities of these technologies in handling and processing large and complex datasets in real-time. The assignments cover various topics such as data ingestion, data processing, data storage, and data analysis, among others. Whether you are a big data enthusiast or a professional looking to build your skills in this field, this repository is a great resource to get started. So, go ahead, fork the repository and start working on these assignments to take your big data skills to the next level!

## DAMG7245_Assignment_1

### Case summary
You are working for a Geospatial startup and are tasked to build a data exploration tool that leverages publicly available data and makes it easier for data analysts to download data. The data sources are on the NOA website and you have chosen the NexRad and GOES satellite datasets for exploration.

### Tasks
- GEOS
    - Explore and download selected datasets for the GOES satellite dataset
    - Given a filename, construct the hyperlink of data location.
    - Write Unit tests for all the use cases
    - Test using the links from [GEOS Dataset Links](https://docs.google.com/spreadsheets/d/1o1CLsm5OR0gH5GHbTsPWAEOGpdqqS49-P5e14ugK37Q/edit#gid=0)

- NexRad
    - Explore and download selected datasets for the NexRad dataset
    - Given a filename, construct the hyperlink of data location.
    - Write Unit tests for all the use cases
    - Test using the links from [NexRad Dataset Links](https://docs.google.com/spreadsheets/d/1o1CLsm5OR0gH5GHbTsPWAEOGpdqqS49-P5e14ugK37Q/edit#gid=651299232)

- Use a python package of your choice and plot the NexRad locations

### Prerequisites
* IDE
* Python 3.x
```bash
python -m venv assgn_venv
source assgn_venv\bin\activate
```

### Process Flow
* Create a python virtual environment and activate
```bash
python -m venv assgn_venv
```

* Activate the virtual environment
```bash
source env/bin/activate  # on Linux/macOS
env\Scripts\activate     # on Windows
```

* Install the required packages from requirements.txt file
```bash
pip install -r requirements.txt
```

* Run Streamlit app
```bash
streamlit run streamlit_main.py
```

* Interacting with AWS S3:
In the Streamlit app, we are using Boto3 library to access AWS S3 bucket and retrieve data file. You will need to provide your AWS credentials to access the S3 bucket. Here's an example of how to access the S3 bucket and retrieve the data file using Boto3:

```bash
import boto3

s3 = boto3.client("s3", aws_access_key_id="<your_access_key>",
                  aws_secret_access_key="<your_secret_key>")
obj = s3.list_objects(Bucket="<your_bucket_name>", Key="<your_file_name>").get('Contents')
```

Test the Streamlit app locally to make sure it's working as expected.

* Deploy the Streamlit app to a cloud platform:
We have deployed the Streamlit app to a cloud platform such as AWS Elastic Beanstalk, Heroku, or Google Cloud Platform. You may need to add a requirements.txt file to your repository to specify the required packages for the deployment.

* CodeLabs Documentation for the assignment flow
We have created the documentation for assignment flow in CodeLabs to get started with the assignment for a new user. Here is the link to the [file](https://codelabs-preview.appspot.com/?file_id=1TBcYrdoadQK-Ls0QfJWQXjo0yJjhgUBBq-kn7Q2gN9E#0).

### Deployed App
App is deployed on Streamlit Cloud and accessed via [link](https://bigdataia-spring2023-team02-assignment-1-streamlit-main-em4swm.streamlit.app)

========================================================================================================================
> WE ATTEST THAT WE HAVEN’T USED ANY OTHER STUDENTS’ WORK IN OUR ASSIGNMENT AND ABIDE BY THE POLICIES LISTED IN THE STUDENT HANDBOOK.
> Meet: 25%, Ajin: 25%, Siddhi: 25%, Akhil: 25%