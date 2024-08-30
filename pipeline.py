from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
# from apify_client import ApifyClient
# import Extraction_model
import os
import dotenv

# dotenv.load_dotenv()

# APIFY_API_KEY = os.getenv('APIFY_API_KEY')
# client = ApifyClient(APIFY_API_KEY)

# #? previous list for initiate
# previous_id = "ApbLPn5CCBdMHtN61"
# previous_house_list = []
# for item in client.dataset(previous_id).iterate_items():
#     previous_house_list.append(item)
def extract_data():
    # Extract new data from Apify API
    print("Extraction")
    test = "test"
    return test

def transform_data(test):
#     for item in house_list:

#         Extraction_model.get_entities()


    # Transform the extracted data
    print(test)

def load_data():
    # Load the transformed data to a destination (e.g., database, file)
    #? get last unit_id
    #? 
    pass

with DAG(
    dag_id='daily_automation_pipeline',
    default_args={
        'owner': 'facebook_group_extraction',
        'start_date': datetime(2024, 8, 30)
    },
    schedule_interval='@daily',
) as dag:

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
    )

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data
    )

    extract_task >> transform_task >> load_task 

dag.run()