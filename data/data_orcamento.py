# Importar Bibliotecas
import pandas as pd
from datetime import datetime
import json
from IPython.display import display
import os

# Função para carregar o arquivo JSON
def load_json(file_name):
    with open(file_name, 'r', encoding='utf-8') as f:
        data = json.load(f)  # Carrega os dados do arquivo JSON
    return data

# Definir o caminho correto para o arquivo JSON na pasta 'files'
# Obter o caminho absoluto do diretório raiz do repositório (AIKON_BANCO_DADOS)
repo_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
file_name = os.path.join(repo_dir, 'files', 'orcamento.json')

# Carregar o JSON e transformar em DataFrame
data = load_json(file_name)
df = pd.DataFrame(data)

# Remover colunas específicas no DataFrame pandas
colunas_remover = ["projects", "pricesByCategory", "scheduledPercentComplete", "percentComplete", "measuredQuantity"]
df_colunas_removidas = df.drop(columns=colunas_remover)

# Explodir a coluna `tasks` no DataFrame pandas
df_tasks_exploded = df_colunas_removidas.explode('tasks')

# Verificar se a coluna `tasks` existe e contém dados
if 'tasks' in df_tasks_exploded.columns:
            df_tasks_exploded['presentationId'] = df_tasks_exploded['tasks'].apply(lambda x: x['presentationId'] if pd.notnull(x) else None)
else:
    df_tasks_exploded['presentationId'] = None

# Preencher os valores `None` em `presentationId` com uma string vazia
df_tasks_exploded['presentationId'] = df_tasks_exploded['presentationId'].fillna('')

# Manter todas as colunas exceto `tasks`
df_final_pandas = df_tasks_exploded.drop(columns=['tasks'])

# Filtrar obras
building_ids = [166] # Filtrar todas as obras que deseja trazer os dados
filtered_df = df_final_pandas[df_final_pandas['buildingId'].isin(building_ids)]

# Função para determinar o nível com base na contagem de caracteres
def determine_wbs_level(wbsCode):
    length = len(wbsCode)
    if length == 2:
        return 'Nível 1'
    elif length == 6:
        return 'Nível 2'
    elif length == 10:
        return 'Nível 3'
    elif length == 14:
        return 'Nível 4'
    else:
        return 'Nível Desconhecido'

# Aplicando a função para criar a nova coluna 'wbs'
filtered_df['wbs'] = filtered_df['wbsCode'].apply(determine_wbs_level)

dtype_dict = {
    'buildingId': 'str', 
    'buildingName': 'str', 
    'buildingStatus': 'str', 
    'versionNumber': 'str',
    'buildingUnitId': 'str', 
    'buildingUnitName': 'str', 
    'id': 'str', 
    'wbsCode': 'str', 
    'workItemId': 'str',
    'description': 'str', 
    'unitOfMeasure': 'str', 
    'quantity': 'float64', 
    'unitPrice': 'float64', 
    'totalPrice': 'float64',
    'baseTotalPrice': 'float64', 
    'presentationId': 'str',
    'wbs': 'str'
}

# Aplicar os tipos de dados ao DataFrame
df_final = filtered_df.astype(dtype_dict)

# Obter a data e hora atual
data_atualizacao = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# Adicionar a coluna 'DataHoraAtualizacao' com o valor da data e hora atual
df_final['DataHoraAtualizacao'] = data_atualizacao

# Caminho para salvar o novo arquivo JSON na pasta 'data'
output_file_name = os.path.join(repo_dir, 'db', 'db_orcamentos.json')

# Salvar o DataFrame em um novo arquivo JSON
df_final.to_json(output_file_name, orient='records', lines=False, force_ascii=False, indent=4)

print(f"DataFrame salvo com sucesso em: {output_file_name}")