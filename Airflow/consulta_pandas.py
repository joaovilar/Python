import pandas as pd

# Substitua o caminho pelo local real do arquivo Churn.csv no seu sistema
file_path = 'C:/Users/jvsilva17/OneDrive - Stefanini/Desktop/airflow/data/Churn.csv'

df = pd.read_csv(file_path)

# Exibir as primeiras linhas dos dados
print(df.head())


