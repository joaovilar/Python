from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.hooks.base_hook import BaseHook
from azure.storage.blob import BlobServiceClient

# Configuração padrão para a DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 8, 10),
    'retries': 0
}

def move_files_to_containers():
    # Configuração das conexões
    src_container = "files"
    dst_container_mapping = {
        "xlsx": "excel",
        "csv": "csv",
        "json": "json",
        "qvd": "qvd"
    }
    
    # Obtém a conexão com o Azure Data Lake
    data_lake_conn = BaseHook.get_connection("azure_data_lake")
    connection_string = data_lake_conn.extra_dejson.get("connection_string", "")
    
    # Verifica se a conexão foi obtida corretamente
    if not connection_string:
        raise ValueError("Azure Data Lake connection string not found.")
    
    # Obtém as informações da conexão
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    container_client = blob_service_client.get_container_client(src_container)
    
    # Listar blobs no container de origem
    blobs_to_move = container_client.list_blobs()
    
    for blob in blobs_to_move:
        blob_name = blob.name
        blob_extension = blob_name.split(".")[-1]
        
        if blob_extension in dst_container_mapping:
            dst_container = dst_container_mapping[blob_extension]
            destination_container_client = blob_service_client.get_container_client(dst_container)
            
            source_blob_client = container_client.get_blob_client(blob_name)
            destination_blob_client = destination_container_client.get_blob_client(blob_name)
            
            destination_blob_client.start_copy_from_url(source_blob_client.url)
            source_blob_client.delete_blob()

# Definição da DAG com nome, configurações padrão e agendamento
dag = DAG(
    'move_files_to_blob_containers_azure',
    default_args=default_args,
    schedule_interval='0 7 * * 1-5',
    catchup=False
)

# Tarefa que move os arquivos para os containers específicos
move_files_task = PythonOperator(
    task_id='move_files_to_containers_azure',
    python_callable=move_files_to_containers,
    dag=dag
)

move_files_task
