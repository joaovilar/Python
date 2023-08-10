import pandas as pd
import psycopg2
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

# Definição das configurações padrão
# Default configuration settings
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 8, 9),
    'retries': 0,
}

# Criação do DAG
# DAG creation
dag = DAG(
    'etl_csv_postgres',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)

def etl_process():
    # Caminho para o arquivo CSV de entrada
    # Path to the input CSV file
    input_file = '/opt/airflow/data/Churn.csv'

    # Conexão com o banco de dados PostgreSQL
    # PostgreSQL database connection
    db_connection = psycopg2.connect(
        host='postgres',
        port=5432,
        user='airflow',
        password='airflow',
        dbname='postgres'
    )

    # Carregar o CSV em um DataFrame
    # Load the CSV into a DataFrame
    df = pd.read_csv(input_file)

    # Tratar valor NaN na coluna D
    # Handle NaN value in column D
    df['D'].fillna('Não Informado', inplace=True)

    # Inicializar o cursor do banco de dados
    # Initialize the database cursor
    cursor = db_connection.cursor()

    # Limpar a tabela antes de inserir os novos dados
    # Clear the table before inserting new data
    clear_table_query = "truncate table public.dados_clientes"
    cursor.execute(clear_table_query)
    
    # Iterar sobre as linhas do DataFrame e inserir os dados na tabela
    # Iterate over DataFrame rows and insert data into the table
    for index, row in df.iterrows():
        sql = """
            INSERT INTO public.dados_clientes (x0, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, dataatualizacao)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        values = (
            str(row['A']), str(row['B']), str(row['C']), str(row['D']), str(row['E']),
            str(row['F']), str(row['G']), str(row['H']), str(row['I']), str(row['J']),
            row['L'], row['M'], datetime.now().date()
        )
        cursor.execute(sql, values)

    # Confirmar as alterações e fechar a conexão com o banco de dados
    # Commit changes and close the database connection
    db_connection.commit()
    db_connection.close()

# Definir a tarefa do ETL como um operador Python
# Define the ETL task as a Python operator
load_data_task = PythonOperator(
    task_id='load_data',
    python_callable=etl_process,
    dag=dag,
)

# Definir dependências das tarefas
# Define task dependencies
load_data_task
