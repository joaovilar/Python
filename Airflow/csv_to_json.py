from airflow import DAG
from datetime import datetime
from big_data_operator import BigDataOperator


dag = DAG('csv_to_json', description='Plugin Big Data',
          schedule_interval='0 7 * * 1-5', start_date=datetime(2023,6,29),
          catchup=False)

transform_csv_to_json = BigDataOperator(
    task_id='csv_to_json',
    path_to_csv_file = '/opt/airflow/data/Churn.csv',
    path_to_save_file = '/opt/airflow/data/Churn.json',
    file_type = 'json',
    dag=dag)

transform_csv_to_json