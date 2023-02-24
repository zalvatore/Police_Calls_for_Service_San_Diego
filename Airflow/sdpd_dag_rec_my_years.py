# test_dag.py
# This code borrows heavily from https://airflow.apache.org/docs/apache-airflow/stable/tutorial.html
from datetime import timedelta, timedelta, date, datetime
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
#from datetime import datetime, timedelta, date, time
#from tqdm import tqdm
from sqlalchemy import create_engine
#from sqlalchemy import text
import pymysql
pymysql.install_as_MySQLdb()
import requests
import mysql.connector
from geopy.geocoders import Nominatim

default_args = {
    'owner': 'Salvador',
    'depends_on_past': False,
    'email': ['salvador@clima2226.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=15),
}
dag = DAG(
    'SDPD_Service_Calls_Years',
    default_args=default_args,
    description='SDPD Service Calls to DB and Geolocations for year timeframe',
    schedule_interval= '@monthly',
    start_date=days_ago(2),
    tags=['SDPD','Long'],
)
### PYTHON FUNCTIONS
def sdpd_load_year():
    
    #For ZipCodes
    #from geopy.extra.rate_limiter import RateLimiter
    
    #DB Connect
    GOOGLE_API_KEY = 'xxxxx'
    user_agent_name = 'Sal-App'
    geolocator = Nominatim(user_agent = user_agent_name)

    database = "sdpd"
    user = 'xxxx'
    password = 'xxxxx
    host = 'sdpd.chck20ykciaw.us-west-2.rds.amazonaws.com'
    Days_back = 400


    #Initial Connection
    cnx = mysql.connector.connect(user=user,
                                password=password,
                                host=host,
                                database=database
                                )
    cursor = cnx.cursor()

    #Connect to DB
    #engine = create_engine(f"mysql://{user}:{password}@{host}/{database}")
    #connection = engine.connect()

    #Load Data
    #Need to create or import current year by default
    year = datetime.now().year;

    url = f"https://seshat.datasd.org/pd/pd_calls_for_service_{year}_datasd.csv"
    df = pd.read_csv(url)

    #changing format from object to datetime
    df['date_time_date'] = pd.to_datetime(df['date_time'], format='%Y-%m-%d')

    #Using current date minus 4 days to get recent data
    current_date = date.today()
    df_recent = df.loc[(df['date_time_date'].dt.date > current_date - timedelta(Days_back))]


    #Data Finder

    def Find_Data(row,cursor,table, key):
        try:
            #print(f"SELECT * from {table} where {key} = '{row[key]}'")
            cursor.execute(f"SELECT * from {table} where {key} = '{row[key]}'")
            # Fetch a record
            result = cursor.fetchone()
        except:
            result = 0
        
        return result


    #Load Calls


    #Calls
    def Load_Calls(row,cursor,table,year):
        # Create a new record
        sql = f"""INSERT INTO {table} ( incident_num,
                                        incident_date_time,
                                        call_type,
                                        dispo_code,
                                        beat,
                                        priority,
                                        incident_year
                                        ) VALUES (%s,%s,%s,%s,%s,%s,%s)"""

        # Execute the query
        cursor.execute(sql, (row['incident_num'],
                            row['date_time'],
                            row['call_type'],
                            row['disposition'],
                            row['beat'],
                            row['priority'],
                            year,
                            )
                    )
        cnx.commit()  
        

    #recent data in dict
    recent_dict = df_recent.to_dict('records')

    #for row in tqdm(recent_dict):
    for row in recent_dict:
        #Load Service Calls
        Table = 'service_calls'
        Key = 'incident_num'
        #Verify if Data is in table
        Data_Found = Find_Data(row,cursor,Table,Key)
        if Data_Found is None:
            #If no data found load
            try:
                Load_Calls(row,cursor,Table,year)
            
            except:
                print(row)
                
        else:
            pass   
        
    ### Address ###

    #Address
    def Load_Calls_Address(row,cursor,table):
        address_list = [row['address_number_primary'],
                row['address_dir_primary'],
                row['address_road_primary'],
                row['address_sfx_primary'],
                        'San Diego CA'
                    ]
        
        def concatenate_elements(elements):
            result = ''
            for element in elements:
                if element is not None and not pd.isnull(element) and element != 0:
                    result += str(element) + ' '
            return result.strip()
        
        
        address = concatenate_elements(address_list)
        
        # Create a new record
        sql = f"""INSERT INTO {table} ( incident_num,
                                        address_number_primary,
                                        address_dir_primary,
                                        address_road_primary,
                                        address_sfx_primary,
                                        address_dir_intersecting,
                                        address_road_intersecting,
                                        address_sfx_intersecting,
                                        address
                                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

        # Execute the query
        cursor.execute(sql, (row['incident_num'],
                            row['address_number_primary'],
                            row['address_dir_primary'],
                            row['address_road_primary'],
                            row['address_sfx_primary'],
                            row['address_dir_intersecting'],
                            row['address_road_intersecting'],
                            row['address_sfx_intersecting'],
                            address
                            )
                    )
        cnx.commit()
        
    for row in recent_dict:
            #Load Service Calls Address        
            Table = 'address'
            Key = 'incident_num'
            #Verify if Data is in table
            Data_Found = Find_Data(row,cursor,Table,Key)
            if Data_Found is None:
                #If no data found load
                try:
                    Load_Calls_Address(row,cursor,Table)
                except:
                    print(row)
                    
                    
            else:
                pass
        
    ###### Lat and Long

    #Connect to DB
    engine = create_engine(f"mysql://{user}:{password}@{host}/{database}")
    connection = engine.connect()


    result = connection.execute(
        """
        SELECT incident_num,address from address
        order by incident_num desc

        """)
    column_names = result.keys()
    rows = result.fetchall()
    result_df = pd.DataFrame(rows, columns=column_names)
    Address_Book = result_df
    Address_Book

    def Lat_Long(address):
        lat, lng  = None, None
        api_key = GOOGLE_API_KEY

        base_url = "https://maps.googleapis.com/maps/api/geocode/json"
        endpoint = f"{base_url}?address={address}&key={api_key}"
        r = requests.get(endpoint)
        if r.status_code not in range(200, 299):
            #error
            return None, None
        try:
            #found
            results = r.json()['results'][0]
            lat = results['geometry']['location']['lat']
            lng = results['geometry']['location']['lng']
            
        except:
            pass
        return lat, lng

    def DF_GeoCode(row):
        column_name = 'address'
        address_value = row[column_name]
        address_lat, address_lng = Lat_Long(address_value)
        row['lat'] = address_lat
        row['lng'] = address_lng
        
        return row

    #Lat and Long load
    def Load_LL(row,cursor,table):
        # Create a new record
        sql = f"""INSERT INTO {table} ( incident_num,
                                        lat,
                                        lng
                                        ) VALUES (%s,%s,%s)"""

        # Execute the query
        cursor.execute(sql, (row['incident_num'],
                            row['lat'],
                            row['lng']
                            )
                    )
        cnx.commit() 

    #Clean Address Book
    Address_Book_noN = Address_Book.dropna()
    Address_Book_noN


    for row in Address_Book_noN.to_dict('records'):
        Data_Found = Find_Data(DF_GeoCode(row),
                            cursor,
                            'geolocations',
                            'incident_num')

        if Data_Found is None:
            try:
                Load_LL(DF_GeoCode(row),
                    cursor,
                    'geolocations')
            except:
                print(row)
                
                
    #### zipcode

    #Get Zipcodes
    result = connection.execute(
        """
        SELECT incident_num,lat,lng from geolocations
        order by incident_num desc

        """)
    column_names = result.keys()
    rows = result.fetchall()
    ZipCode_Book = pd.DataFrame(rows, columns=column_names)
    ZipCode_Book = ZipCode_Book.dropna()
    ZipCode_Book       

    #Functions
    def get_zipcode(df, geolocator, lat_field, lon_field):
        try:
            location = geolocator.reverse((df[lat_field], df[lon_field]))
        #print(location)
            df['Zipcode'] = location.raw['address']['postcode']
        except:
            df['Zipcode'] = 0
        #print('Error')
        #print(df)
        return df

    #Lat and Long
    def Load_Zipcode(row,cursor,table):
        # Create a new record
        sql = f"""INSERT INTO {table} ( incident_num,
                                        Zipcode
                                        ) VALUES (%s,%s)"""

        # Execute the query
        cursor.execute(sql, (row['incident_num'],
                            row['Zipcode']
                            )
                    )
        cnx.commit()

    for row in ZipCode_Book.to_dict('records'):
        Data_Found = Find_Data(row,
                            cursor,
                            'zipcodes',
                            'incident_num')

        if Data_Found is None:
            #If no data found load 
            try:
                Load_Zipcode(get_zipcode(row,
                                geolocator=geolocator,
                                        lat_field='lat',
                                        lon_field='lng'
                                        ),
                        cursor,
                        'zipcodes')
            except:
                print(row)
        else:
            pass     
    print('SDPD Data Loaded')

### OPERATORS
t1 = PythonOperator(
    task_id="task_load_year",
    python_callable=sdpd_load_year,
    dag=dag
)
