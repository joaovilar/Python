from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import psycopg2

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 8, 10),
    'retries': 0
}

dag = DAG(
    'carregar_planos_para_dw',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)

def limpar_tabelas():
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

        # Limpar tabela 'dm_planos'
        with conn_dw.cursor() as cur_dw:
            cur_dw.execute("truncate table dm_planos")
            conn_dw.commit()

        # Limpar tabela 'dm_beneficiarios'
        with conn_dw.cursor() as cur_dw:
            cur_dw.execute("truncate table dm_beneficiarios")
            conn_dw.commit()

    except Exception as e:
        print(f"Erro ao limpar tabelas: {e}")
    finally:
        if conn_dw is not None:
            conn_dw.close()

def carregar_planos_para_dw():
    conn_pg = None
    conn_dw = None
    try:
        # Conexão com o banco de dados 'postgres'
        conn_pg = psycopg2.connect(
            dbname='postgres',
            user='airflow',
            password='airflow',
            host='postgres',
            port='5432'
        )
        
        # Conexão com o banco de dados 'DW_SAUDE'
        conn_dw = psycopg2.connect(
            dbname='DW_SAUDE',
            user='airflow',
            password='airflow',
            host='postgres',
            port='5432'
        )

        # Limpar tabelas antes da carga
        limpar_tabelas()

        # Carregar dados da tabela 'Planos' do banco de dados 'postgres' para 'DW_SAUDE'
        with conn_pg.cursor() as cur_pg, conn_dw.cursor() as cur_dw:
            cur_pg.execute("SELECT * FROM Planos")
            rows = cur_pg.fetchall()
            for row in rows:
                cur_dw.execute("INSERT INTO dm_planos (plano_id, nome, tipo, mensalidade) VALUES (%s, %s, %s, %s)",
                               (row[0], row[1], row[2], row[3]))
            conn_dw.commit()

        # Carregar dados da tabela 'Beneficiarios' do banco de dados 'postgres' para 'DW_SAUDE'
        with conn_pg.cursor() as cur_pg, conn_dw.cursor() as cur_dw:
            cur_pg.execute("""select b.nome, b.idade, p.nome nome_plano, b.data_contratacao  
                                from public.beneficiarios b
                                inner join planos p on b.plano_id =p.plano_id """)
            rows = cur_pg.fetchall()
            for row in rows:
                cur_dw.execute("INSERT INTO dm_beneficiarios (nome, idade, nome_plano, data_contratacao) VALUES (%s, %s, %s, %s)",
                               (row[0], row[1], row[2], row[3]))
            conn_dw.commit()
            
    except Exception as e:
        print(f"Erro: {e}")
    finally:
        if conn_pg is not None:
            conn_pg.close()
        if conn_dw is not None:
            conn_dw.close()

limpar_tabelas_task = PythonOperator(
    task_id='limpar_tabelas',
    python_callable=limpar_tabelas,
    dag=dag
)

executar_carregamento = PythonOperator(
    task_id='executar_carregamento',
    python_callable=carregar_planos_para_dw,
    dag=dag
)

limpar_tabelas_task >> executar_carregamento
