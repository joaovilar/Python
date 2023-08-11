from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import pandas as pd
import os

# Configuração das credenciais do banco de dados PostgreSQL
# Configuration of PostgreSQL database credentials
db_credentials = {
    'dbname': 'postgres',
    'user': 'airflow',
    'password': 'airflow',
    'host': 'postgres',
    'port': '5432'  # Porta padrão do PostgreSQL
}

# Local onde os arquivos .parquet serão salvos
# Location where .parquet files will be saved
output_folder = '/opt/airflow/data/Parquet/'

def extract_and_convert_all_tables():
    conn = psycopg2.connect(**db_credentials)
    query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
    tables = pd.read_sql_query(query, conn)['table_name']
    
    for table in tables:
        query = f'SELECT * FROM public.{table}'
        df = pd.read_sql(query, conn)
        
        output_file = os.path.join(output_folder, f'{table}_{datetime.now().strftime("%Y-%m-%d")}.parquet')
        df.to_parquet(output_file, index=False)
    
    conn.close()

# Configuração padrão para a DAG
# Default configuration for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 8, 10),
    'retries': 0
}

# Definição da DAG com nome, configurações padrão e agendamento
# Definition of the DAG with name, default settings, and scheduling
dag = DAG(
    'tables_to_parquet',
    default_args=default_args,
    schedule_interval='0 7 * * 1-5',  # Executar de segunda à sexta-feira às 07:00
    catchup=False
)

# Tarefa que executa a conversão de todas as tabelas para .parquet
# Task that executes the conversion of all tables to .parquet
convert_all_tables_task = PythonOperator(
    task_id='converter_tabelas_para_parquet',
    python_callable=extract_and_convert_all_tables,
    dag=dag
)

# Definir a ordem das tarefas na DAG (somente a tarefa de conversão neste caso)
# Define the order of tasks in the DAG (only the conversion task in this case)
convert_all_tables_task
