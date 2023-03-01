SD PD Calls Data

Source Files: http://seshat.datasd.org/pd/

The goal for this project is to create an ETL Automation for the San Diego Police Department Service Calls Data, and use that data to make visualizations.

The data was loaded from the SD PD Site. Using Python Libraries of mysql.connector, pandas, datetime, tqdm, sqlalchemy, pymysql, requests, geopy.extra.rate_limiter, and geopy.geocoders, we connected with our AWS MySQL Instance and imported the CSV Files via provided URLs. After connecting using a cursor as connection point for mysql.connector, we created a function to avoid duplicate data uploads. That way everytime we run our Load Data script, it would add on the most recent call data from the CSV files provided by the PD. 

The script is being executed using Apache Airflow. Airflow is a platform to programmatically author, schedule and monitor workflows. We connected our Airflow with the AWS MySQL Database instance to store our data into. The script is scheduled to run automatically to get daily data. 

That data is then accessed by the DASH App. Dash apps give a point-&-click interface to models written in Python, vastly expanding the notion of what's possible in a traditional "dashboard." With Dash apps, data scientists and engineers put complex Python analytics in the hands of business decision-makers and operators.

We used multiple views in our database to help visualize the data and present it on the DASH. Some of those views are calls per zip code, to realize the most distressed location within San Diego. We can also visualize based on different dispositions and beats. 

Requirements ->

--constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.4.3/constraints-3.10.txt"
apache-airflow[package-extra]
apache-airflow-providers-amazon
boto3
SQLAlchemy-JSONField
SQLAlchemy-Utils
SQLAlchemy
mypy-extensions
mypy
mysql-connector-python-rf
pandas
pyodbc
tqdm
SQLAlchemy
PyMySQL
requests
types-PyMySQL
apache-airflow-providers-mysql
mysqlclient
mysql-connector-python
geopy
