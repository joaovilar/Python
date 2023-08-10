import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


def etl_process():
    input_file = '/opt/airflow/data/Churn.csv'
    output_file = '/opt/airflow/data/Processados/Churn_{}.csv'.format(datetime.now().strftime('%Y-%m-%d'))

    # Carregar a planilha CSV
    df = pd.read_csv(input_file)

    # Adicionar a nova coluna com a data atual
    df['current_date'] = datetime.now().strftime('%Y-%m-%d')

    # Salvar a nova planilha com o nome desejado
    df.to_csv(output_file, index=False)
    print('Nova planilha salva em:', output_file)

# Definir argumentos do DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023,8,7),
    'depends_on_past': False,
    'retries': 1,
}

# Criar o DAG
dag = DAG('etl_churn_csv', 
          description='cria csv com nova coluna de data',
          schedule_interval=None, # Define a frequência de execução do DAG (None para execução manual)
          start_date=datetime(2023,6,29), 
          catchup=False # Não execute tarefas em atraso ao agendar o DAG pela primeira vez
          )

# Criar uma tarefa PythonOperator
run_etl = PythonOperator(
    task_id='run_etl',
    python_callable=etl_process,
    dag=dag,
)

# Definir a ordem das tarefas
run_etl
