# Data Pipelines with Airflow

### Background
The music streaming startup, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

This project will build a data pipline to process the raw data from Amazon S3 Bucket to the Amazon Redshift data warehouse and run some data quality checks to ensure the pipeline has processed the data correctly. 


### Datasets
**Song Dataset:** The song data is from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.

 - song_data/A/B/C/TRABCEI128F424C983.json
 - song_data/A/A/B/TRAABJL12903CDCF1A.json
And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like.
`{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}`

**Log Dataset:** The Spariky streaming app generates activity logs partitioned by year and month. For example, here are filepaths to two files in this dataset.

 - log_data/2018/11/2018-11-12-events.json
 - log_data/2018/11/2018-11-13-events.json

### Process
The Airflow data pipeline process is intended to process the raw data and model it into a data warehouse to facilitate the analytics team to gain insight into their users' behavior. 

The data pipeline is built with the following components:
 - create_tables.sql: Contains the SQL to create the database schema (if it does not previously exist)
 - sql_queries.py: Contains the insert queries stored int a Python class
 - udac_example_dag.py: Contains the code for the Airflow DAG
 - stage_redshift.py: Contains the Python operator to extract the raw data from S3 and write to staging tables in Redshift
 - load_fact.py: Contains the Python operator to generate the fact table in Redshift
 - load_dimension.py: Contains the Python operator to generate the dimension tables from the staging tables 
 - data_quality.py: Contains the Python operator to perform data quality checks 
 
**Airflow DAG Pipeline Diagram**

<img src="example-dag.png">

### Setup 
1. Set up Amazon Redshift data warehouse
1. Run SQL queries from `create_tables.sql` file if the data warehouse has not been created yet
1. Configure Airflow connections with Amazon credentials
    - `Conn Id` = `aws_credentials`
    - `Conn Type` = `Amazon Web Services`
    - `Login` = `Access key ID` for the IAM User
    - `Password` = `Secret access key` for the IAM User
1. Configure the Airflow connections with the Redshift credentials
 - `Conn Id`: `redshift`
 - `Conn Type`: `PostgreSQL`
 - `Host`: Redshift cluster endpoint 
 - `Schema`: Database name (sparkify)
 - `Login`: Username with privlages on the database
 - `Password`: Database password
 - `Port`: Redshift port (typically `5439`)