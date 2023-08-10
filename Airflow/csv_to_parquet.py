from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
import pandas as pd

def csv_to_parquet():
    input_file = '/opt/airflow/data/Churn.csv'
    output_file = '/opt/airflow/data/Parquet/Churn_{}.parquet'.format(datetime.now().strftime('%Y-%m-%d'))

    # Carregar a planilha CSV
    df = pd.read_csv(input_file)

    # Salvar a planilha em formato Parquet
    df.to_parquet(output_file, index=False)
    print('Arquivo Parquet salvo em:', output_file)

# Definir argumentos do DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 8, 7),
    'depends_on_past': False,
    'retries': 1,
}

# Criar o DAG
dag = DAG(
    'csv_to_parquet',
    default_args=default_args,
    schedule_interval=None,  # Define a frequência de execução do DAG (None para execução manual)
    catchup=False,  # Não execute tarefas em atraso ao agendar o DAG pela primeira vez
)

# Criar uma tarefa PythonOperator
convert_to_parquet = PythonOperator(
    task_id='convert_to_parquet',
    python_callable=csv_to_parquet,
    dag=dag,
)

# Definir a ordem das tarefas
convert_to_parquet
