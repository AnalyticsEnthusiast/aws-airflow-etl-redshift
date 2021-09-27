### AWS Redshift with Airflow ETL

**Name: Darren Foley**

**Email: darren.foley@ucdconnect.ie**

**Date: 2021-09-26**

<br>

#### Project Overview

<p>Sparkify have decided to implement apache airflow as their main tool of choice in order to improve the efficency of their batch data pipelines for loading log and song data into their AWS Redshift data warehouse. The following project is a proof of concept (POC) to demonstrate the viability of Airflow as a production ETL tool within Sparkify.</p> 

<br>

#### Project Description

Main project files

<br>

| File Name             | File Type  | Description                                 |  
|----------------------:|:----------:|:-------------------------------------------:| 
| create_tables.sql     | SQL        | List of SQL DDL SQL Queries                 |
| sparkify_dag.py       | python     | Main Airflow DAG                            |
| sql_create_tables.py  | python     | Python Class with Create table statements   |
| sql_insert_queries.py | python     | Python Class to insert data into tables     |
| sql_quality_check.py  | python     | Python Class for performing data quailty SQL|
| data_quality.py       | python     | Airfow Operator to check data quality       |
| load_dimension.py     | python     | Airflow Operator to load dimension tables   |
| load_fact.py          | python     | Airflow Operator to load fact table         |
| stage_redshift.py     | python     | Airflow Operator to load staging tables     |

<br>

### Project Setup

Airflow Variable Setup:

1. "aws_credentials" - Contains the Key and Secret key of user allowed to access s3 on behalf of Redshift.

2. "redshift"- Contains details of redshift cluster, hostname, port, schema, username & password

<br>

For test purposes the following variables have been hard coded just for POC. In a normal production environment these would be parameterized:

In the **StageToRedshiftOperator** the year and month variables have been hard coded in the DAG to "2018" and "11". In production these would be dynamically generated to the year and month of the execution date.

In the **LoadFactOperator** the test variable is set to "true". This truncates the fact table before loading which makes testing much easier. In a production system this would would be set to "false" as fact tables are too large to truncate each time.

In the python file **sql_create_tables.py** the SQL statements contain "DROP TABLE IF EXISTS public.table_name;". This is for test purposes only, not suitable for production tables.

<br>

Redshift Networking and region location:

1. Ensure that the Redhsift cluster is geographically located near the source s3 buckets, in this case us-west-2.

2. Ensure that the cluster host is visible to airflow outbound over port 5439.
