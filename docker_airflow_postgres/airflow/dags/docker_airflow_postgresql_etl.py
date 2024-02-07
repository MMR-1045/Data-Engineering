#import modules
import pandas as pd
from airflow import DAG
import airflow
from airflow.operators.python import PythonOperator
import numpy as np
import requests
from datetime import datetime
import psycopg2
import json
from sqlalchemy import create_engine

# define database cedentials for database connection
HOST_NAME = 'wine_db'
DATABASE = 'wine_db'
USER_NAME = 'admin'
PASSWORD = 'admin'
PORT_ID = 5432
db_connection_string = f"postgresql+psycopg2://{USER_NAME}:{PASSWORD}@{HOST_NAME}:{PORT_ID}/{DATABASE}"

# define a dag variable based on the Airflow DAG object
dag = DAG(
    dag_id='docker_airflow_postgresql_etl',
    start_date=airflow.utils.dates.days_ago(0),
    schedule_interval=None

)

# Function to Extract raw data
def get_recall_data(**kwargs):
    # Extraction
    wine_url = "https://archive.ics.uci.edu/ml/machine-learning-databases/wine/wine.data"
    wine_data = pd.read_csv(wine_url, header=None)

    wine_quality_url = "https://archive.ics.uci.edu/ml/machine-learning-databases/wine-quality/winequality-red.csv"
    wine_quality_data = pd.read_csv(wine_quality_url, sep=";")

    # Convert to dictionary for XCom serialization
    wine_data_dict = wine_data.to_dict(orient='split')
    wine_quality_dict = wine_quality_data.to_dict(orient='split')

    return wine_data_dict, wine_quality_dict

def clean_recall_data(**kwargs):
    # Accessing the output of get_raw_data task
    output = kwargs['ti'].xcom_pull(task_ids='get_raw_data')

    # Extracting wine_data and wine_quality_data from the output
    wine_data_dict, wine_quality_dict = output[0], output[1]

    # Convert back to DataFrame
    wine_data = pd.DataFrame(**wine_data_dict)
    wine_quality_data = pd.DataFrame(**wine_quality_dict)

    # Assigning meaningful column names
    wine_data.columns = ['class', 'alcohol', 'malic acid', 'ash','alcalinity of ash', 'magnesium', 'total phenols','flavonoids', 'nonflavonoid phenols', 'proanthocyanidins','color intensity', 'hue', 'OD280/OD315 of diluted wines','proline']

    # Converting Class column into categorical datatype
    wine_data['class'] = wine_data['class'].astype('category')

    # Checking for any missing values in both datasets
    wine_data_missing = wine_data.isnull().sum()
    wine_quality_missing = wine_quality_data.isnull().sum()

    # Normalizing 'alcohol' column in the wine_data using Min-Max normalization
    wine_data['alcohol'] = (wine_data['alcohol'] - wine_data['alcohol'].min()) / (wine_data['alcohol'].max() - wine_data['alcohol'].min())

    # Creating an average quality column in wine_quality_data
    wine_quality_data['average_quality'] = wine_quality_data[['fixed acidity', 'volatile acidity', 'citric acid','residual sugar', 'chlorides', 'free sulfur dioxide','total sulfur dioxide', 'density', 'pH', 'sulphates','alcohol']].mean(axis=1)

    # Creating a 'quality_label' column based on 'average_quality'
    wine_quality_data['quality_label'] = pd.cut(wine_quality_data['average_quality'], bins=[0, 5, 7, np.inf], labels=['low', 'medium', 'high'])

    # Serialize dictionaries to JSON
    wine_data_json = json.dumps(wine_data_dict)
    wine_quality_json = json.dumps(wine_quality_dict)

    # # Return serialized data
    # return wine_data_json, wine_quality_json
    # Return serialized data
    return {'wine_data_json': wine_data_json, 'wine_quality_json': wine_quality_json}



# define a function to load the data to postgresql database
# and use pd.sql() to load the transformed data to the database directly
# Function to Load data to Database

def load_to_db(**kwargs):
    xcom_push_dict = kwargs['ti'].xcom_pull(task_ids='clean_raw_data')

# Check if the keys are present
    if 'wine_data_json' in xcom_push_dict and 'wine_quality_json' in xcom_push_dict:
        wine_data_json = xcom_push_dict['wine_data_json']
        wine_quality_json = xcom_push_dict['wine_quality_json']

        # Deserialize JSON to dictionaries
        wine_data_dict = json.loads(wine_data_json)
        wine_quality_dict = json.loads(wine_quality_json)

        # Convert dictionaries to DataFrames
        wine_data = pd.DataFrame(**wine_data_dict)
        wine_quality_data = pd.DataFrame(**wine_quality_dict)

        # Create SQLAlchemy engine
        engine = create_engine(db_connection_string)
        # Loading
        # Saving the transformed data to a database table
        wine_data.to_sql('wine_dataset', engine, schema='wine',index=False, if_exists='replace')
        wine_quality_data.to_sql('wine_quality_dataset', engine, schema='wine',index=False, if_exists='replace')
        print(f"Success: Loaded {len(wine_data)} recall records to {DATABASE}.")
        print(f"Success: Loaded {len(wine_quality_data)} recall records to {DATABASE}.")
    else:
        print("XCom values are missing.")

# define task using Airflow PythonOperator for raw data extraction
get_raw_data = PythonOperator(
    task_id='get_raw_data',
    python_callable=get_recall_data,
    provide_context=True,  # Set this to True to provide the task instance context
    dag=dag,
)

clean_raw_data = PythonOperator(
    task_id='clean_raw_data',
    python_callable=clean_recall_data,
    provide_context=True,  # This is necessary for passing context to the callable
    dag=dag,
)

load_data_to_db = PythonOperator(
    task_id='load_data_to_db',
    python_callable=load_to_db,
    op_kwargs={
        'db_connection_string': db_connection_string,
    },
    dag=dag,
)

# set the order to execute each of the tasks in the DAG
get_raw_data >> clean_raw_data >> load_data_to_db