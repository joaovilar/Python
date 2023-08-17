from airflow import DAG
from airflow.operators.email import EmailOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import psycopg2

# Configurações da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 15),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'sql_to_csv_and_email',
    default_args=default_args,
    schedule_interval='0 8 * * 1-5', # Agendamento para segunda a sexta-feira às 08:00
    catchup=False,
)

# Credenciais do banco de dados PostgreSQL
db_credentials = {
    'dbname': 'postgres',
    'user': 'airflow',
    'password': 'airflow',
    'host': 'postgres',
    'port': '5432'
}

# Função para executar a consulta SQL e criar o arquivo CSV
def execute_sql_and_create_csv(**kwargs):
    conn = psycopg2.connect(**db_credentials)
    
    # Consulta SQL
    sql = """
    select b.nome, b.idade,b.data_contratacao,p.nome plano, p.tipo, p.mensalidade 
    from beneficiarios b
    inner join planos p on b.plano_id = p.plano_id
    """

    # Conectar ao banco de dados e executar a consulta
    df = pd.read_sql(sql, conn)
    
    # Caminho de saída do arquivo CSV
    csv_file_path = '/tmp/relatorio_beneficiarios.csv'
    
    # Criar arquivo CSV a partir do DataFrame
    df.to_csv(csv_file_path, index=False)
    
    # Fechar a conexão com o banco de dados
    conn.close()

# Tarefa para executar a consulta SQL e criar o arquivo CSV
sql_and_create_csv = PythonOperator(
    task_id='execute_sql_and_create_csv',
    python_callable=execute_sql_and_create_csv,
    provide_context=True,
    dag=dag,
)

# Tarefa para enviar o arquivo CSV por e-mail
send_email_task = EmailOperator(
    task_id='send_email',
    to=['You e-mail'],  # Informe seu Endereço de e-mail do destinatário
    subject='Relatório de Beneficiários de Planos de Saúde',
    html_content='''
        <html>
        <head>
            <style>
                body {
                    font-family: Arial, sans-serif;
                    background-color: #f4f4f4;
                    margin: 0;
                    padding: 20px;
                }
                .container {
                    max-width: 600px;
                    margin: 0 auto;
                    background-color: #fff;
                    padding: 20px;
                    border-radius: 5px;
                    box-shadow: 0px 2px 5px rgba(0, 0, 0, 0.1);
                }
                .header {
                    background-color: #007BFF;
                    color: #fff;
                    padding: 10px 0;
                    text-align: center;
                    border-top-left-radius: 5px;
                    border-top-right-radius: 5px;
                }
                .content {
                    margin-top: 20px;
                }
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h2>Relatório de Beneficiários de Planos de Saúde</h2>
                </div>
                <div class="content">
                    <p>Olá!</p>
                    <p>Estamos enviando o relatório de beneficiários de planos de saúde em formato CSV.</p>
                    <p>Este relatório contém informações importantes sobre os beneficiários e seus planos de saúde.</p>
                    <p>Por favor, verifique o anexo para mais detalhes.</p>
                    <p>Atenciosamente,</p>
                    <p>Sua Equipe de Dados</p>
                </div>
            </div>
        </body>
        </html>
    ''',
    files=['/tmp/relatorio_beneficiarios.csv'],  # Caminho completo do arquivo CSV criado
    dag=dag,
)


# Definir dependências das tarefas
sql_and_create_csv >> send_email_task
