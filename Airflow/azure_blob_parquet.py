from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import pandas as pd
import os
import pyarrow as pa
import pyarrow.parquet as pq
from airflow.contrib.hooks.wasb_hook import WasbHook
from airflow.hooks.base_hook import BaseHook
from azure.storage.blob import ContentSettings

# Configuração das credenciais do banco de dados PostgreSQL
db_credentials = {
    'dbname': 'postgres',
    'user': 'airflow',
    'password': 'airflow',
    'host': 'postgres',
    'port': '5432'
}

def extract_and_convert_all_tables():
    conn = psycopg2.connect(**db_credentials)
    query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
    tables = pd.read_sql_query(query, conn)['table_name']
    
    data_lake_conn = BaseHook.get_connection("azure_data_lake")
    storage_account_name = "dslakestorage"
    container_name = "postgres-parquet"
    
    wasb_hook = WasbHook(wasb_conn_id="azure_data_lake")

    for table in tables:
        query = f'SELECT * FROM public.{table}'
        df = pd.read_sql(query, conn)

        if not df.empty:
            # Definir o caminho de saída corretamente
            output_local_path = os.path.join("/tmp", f"{table}_{datetime.now().strftime('%Y-%m-%d')}.parquet")
            
            # Converter o DataFrame para uma tabela PyArrow
            arrow_table = pa.Table.from_pandas(df)
            
            # Escrever a tabela em um arquivo Parquet
            pq.write_table(arrow_table, output_local_path, compression='snappy')
            
            # Carregar o arquivo gerado para o Azure Data Lake
            target_path = f"{container_name}/{table}_{datetime.now().strftime('%Y-%m-%d')}.parquet"
            blob_name = f"{table}_{datetime.now().strftime('%Y-%m-%d')}.parquet"

            # Substituir o arquivo existente no Azure Data Lake (se houver)
            content_settings = ContentSettings(content_type="application/octet-stream")
            wasb_hook.load_file(output_local_path, target_path, blob_name=blob_name, content_settings=content_settings, overwrite=True)
    
    conn.close()

# Configuração padrão para a DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 8, 10),
    'retries': 0
}

# Definição da DAG com nome, configurações padrão e agendamento
dag = DAG(
    'postgres_to_data_lake',
    default_args=default_args,
    schedule_interval='0 7 * * 1-5',
    catchup=False
)

# Tarefa que executa a conversão de todas as tabelas para .parquet
convert_all_tables_task = PythonOperator(
    task_id='extract_and_convert_all_tables_to_azure',
    python_callable=extract_and_convert_all_tables,
    dag=dag
)

convert_all_tables_task
