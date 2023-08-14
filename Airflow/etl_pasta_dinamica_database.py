from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import psycopg2
import os
import pandas as pd

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 8, 14),
    'retries': 0,
    'schedule_interval': '0 6 * * 1-5'  # Agendamento para segunda a sexta-feira às 06:00
}

dag = DAG(
    'pasta_csv_para_database',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)

def ler_csv_pasta_dinamica():
    # Ler todos os arquivos CSV na pasta
    csv_folder = '/opt/airflow/data/CSV'
    dfs = []
    for csv_file in os.listdir(csv_folder):
        if csv_file.endswith('.csv'):
            csv_path = os.path.join(csv_folder, csv_file)
            df = pd.read_csv(csv_path)
            dfs.append(df)
    return pd.concat(dfs)

def carregar_csv_para_dw():
    conn_dw = None
    try:
        # Conexão com o banco de dados 'DW_SAUDE'
        conn_dw = psycopg2.connect(
            dbname='DW_SAUDE',
            user='airflow',
            password='airflow',
            host='postgres',
            port='5432'
        )
        
        # Ler dados dos arquivos CSV
        df = ler_csv_pasta_dinamica()
        
        # Adicionar a coluna 'data_atualizacao' com a data do dia corrente
        df['data_atualizacao'] = datetime.now().strftime('%Y-%m-%d')

        # Inserir os dados na tabela 'dm_beneficiarios' no PostgreSQL
        with conn_dw.cursor() as cur_dw:
            for index, row in df.iterrows():
                # Verificar se o beneficiario_id já existe na tabela
                cur_dw.execute("SELECT COUNT(*) FROM dm_beneficiarios WHERE beneficiario_id = %s", (row['beneficiario_id'],))
                count = cur_dw.fetchone()[0]
                if count == 0:
                    cur_dw.execute("INSERT INTO dm_beneficiarios (beneficiario_id, nome, idade, nome_plano, data_contratacao, data_atualizacao) VALUES (%s, %s, %s, %s, %s, %s)",
                                   (row['beneficiario_id'], row['nome'], row['idade'], row['plano_id'], row['data_contratacao'], row['data_atualizacao']))
            conn_dw.commit()
            
    except Exception as e:
        print(f"Erro: {e}")
    finally:
        if conn_dw is not None:
            conn_dw.close()

ler_csv_pasta_task = PythonOperator(
    task_id='ler_csv_pasta',
    python_callable=ler_csv_pasta_dinamica,
    dag=dag
)

carregar_tabela_task = PythonOperator(
    task_id='carregar_tabela',
    python_callable=carregar_csv_para_dw,
    dag=dag
)

ler_csv_pasta_task >> carregar_tabela_task
