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
file_name = os.path.join(repo_dir, 'files', 'mov_bancarias.json')

# Carregar o JSON e transformar em DataFrame
data = load_json(file_name)
df = pd.DataFrame(data)

# Processo para Armazenar a Tabela de Movimentações Bancárias no Lakehouse!!!

# Remover colunas específicas no DataFrame pandas
colunas_remover = ["holdingId", "holdingName", "subsidiaryId", "subsidiaryName", "departamentCosts", "buldingCosts"]
df_colunas_removidas = df.drop(columns=colunas_remover)

# Explodir a coluna `financialCategories` no DataFrame pandas
df_expandir_categorias = df_colunas_removidas.explode('financialCategories')

# Verificar se a coluna `financialCategories` existe e contém dados
if 'financialCategories' in df_expandir_categorias.columns:
    df_expandir_categorias['costCenterId'] = df_expandir_categorias['financialCategories'].apply(lambda x: str(x['costCenterId']) if pd.notnull(x) and 'costCenterId' in x else None)
    df_expandir_categorias['costCenterName'] = df_expandir_categorias['financialCategories'].apply(lambda x: x['costCenterName'] if pd.notnull(x) and 'costCenterName' in x else None)
    df_expandir_categorias['financialCategoryId'] = df_expandir_categorias['financialCategories'].apply(lambda x: x['financialCategoryId'] if pd.notnull(x) and 'financialCategoryId' in x else None)
    df_expandir_categorias['financialCategoryName'] = df_expandir_categorias['financialCategories'].apply(lambda x: x['financialCategoryName'] if pd.notnull(x) and 'financialCategoryName' in x else None)
    df_expandir_categorias['financialCategoryRate'] = df_expandir_categorias['financialCategories'].apply(lambda x: float(x['financialCategoryRate']) if pd.notnull(x) and 'financialCategoryRate' in x else None)
    df_expandir_categorias['projectId'] = df_expandir_categorias['financialCategories'].apply(lambda x: x['projectId'] if pd.notnull(x) and 'projectId' in x else None)
    df_expandir_categorias['projectName'] = df_expandir_categorias['financialCategories'].apply(lambda x: x['projectName'] if pd.notnull(x) and 'projectName' in x else None)
else:
    df_expandir_categorias['costCenterId'] = None
    df_expandir_categorias['costCenterName'] = None
    df_expandir_categorias['financialCategoryId'] = None
    df_expandir_categorias['financialCategoryName'] = None
    df_expandir_categorias['financialCategoryRate'] = None
    df_expandir_categorias['projectId'] = None
    df_expandir_categorias['projectName'] = None

# Preencher os valores `None` com valores padrão
df_expandir_categorias['costCenterId'] = df_expandir_categorias['costCenterId'].fillna('')
df_expandir_categorias['costCenterName'] = df_expandir_categorias['costCenterName'].fillna('')
df_expandir_categorias['financialCategoryId'] = df_expandir_categorias['financialCategoryId'].fillna('')
df_expandir_categorias['financialCategoryName'] = df_expandir_categorias['financialCategoryName'].fillna('')
df_expandir_categorias['financialCategoryRate'] = df_expandir_categorias['financialCategoryRate'].fillna(0.0)
df_expandir_categorias['projectId'] = df_expandir_categorias['projectId'].fillna('')
df_expandir_categorias['projectName'] = df_expandir_categorias['projectName'].fillna('')

# Manter todas as colunas exceto `financialCategories`
df_final_pandas = df_expandir_categorias.drop(columns=['financialCategories'])

# Filtrar empresas
company_ids = [66] # Filtrar todas as empresas que deseja retornar os dados
df_final_pandas = df_final_pandas[df_final_pandas['companyId'].isin(company_ids)]

# Processo para Armazenar a Tabela de Movimentações Bancárias com Apropriações no Lakehouse!!!

# Remover colunas específicas no DataFrame pandas
colunas_remover_aprop = ["holdingId", "holdingName", "subsidiaryId", "subsidiaryName", "departamentCosts"]
df_colunas_removidas_aprop = df.drop(columns=colunas_remover_aprop)

# Explodir a coluna `financialCategories` no DataFrame pandas
df_expandir_aprop = df_colunas_removidas_aprop.explode('financialCategories')

# Explodir a coluna `buldingCosts` no DataFrame pandas
df_expandir_aprop = df_expandir_aprop.explode('buldingCosts')

# Verificar se a coluna `financialCategories` existe e contém dados
if 'financialCategories' in df_expandir_aprop.columns:
    df_expandir_aprop['costCenterId'] = df_expandir_aprop['financialCategories'].apply(lambda x: x['costCenterId'] if isinstance(x, dict) and 'costCenterId' in x else None)
    df_expandir_aprop['costCenterName'] = df_expandir_aprop['financialCategories'].apply(lambda x: x['costCenterName'] if isinstance(x, dict) and 'costCenterName' in x else None)
    df_expandir_aprop['financialCategoryId'] = df_expandir_aprop['financialCategories'].apply(lambda x: x['financialCategoryId'] if isinstance(x, dict) and 'financialCategoryId' in x else None)
    df_expandir_aprop['financialCategoryName'] = df_expandir_aprop['financialCategories'].apply(lambda x: x['financialCategoryName'] if isinstance(x, dict) and 'financialCategoryName' in x else None)
    df_expandir_aprop['financialCategoryRate'] = df_expandir_aprop['financialCategories'].apply(lambda x: x['financialCategoryRate'] if isinstance(x, dict) and 'financialCategoryRate' in x else None)
    df_expandir_aprop['projectId'] = df_expandir_aprop['financialCategories'].apply(lambda x: x['projectId'] if isinstance(x, dict) and 'projectId' in x else None)
    df_expandir_aprop['projectName'] = df_expandir_aprop['financialCategories'].apply(lambda x: x['projectName'] if isinstance(x, dict) and 'projectName' in x else None)

else:
    df_expandir_aprop['costCenterId'] = None
    df_expandir_aprop['costCenterName'] = None
    df_expandir_aprop['financialCategoryId'] = None
    df_expandir_aprop['financialCategoryName'] = None
    df_expandir_aprop['financialCategoryRate'] = None
    df_expandir_aprop['projectId'] = None
    df_expandir_aprop['projectName'] = None

# Verificar se a coluna `buldingCosts` existe e contém dados
if 'buldingCosts' in df_expandir_aprop.columns:
    df_expandir_aprop['buildingId'] = df_expandir_aprop['buldingCosts'].apply(lambda x: x['buildingId'] if isinstance(x, dict) and 'buildingId' in x else None)
    df_expandir_aprop['buildingName'] = df_expandir_aprop['buldingCosts'].apply(lambda x: x['buildingName'] if isinstance(x, dict) and 'buildingName' in x else None)
    df_expandir_aprop['buildingUnitId'] = df_expandir_aprop['buldingCosts'].apply(lambda x: x['buildingUnitId'] if isinstance(x, dict) and 'buildingUnitId' in x else None)
    df_expandir_aprop['costEstimationSheetId'] = df_expandir_aprop['buldingCosts'].apply(lambda x: x['costEstimationSheetId'] if isinstance(x, dict) and 'costEstimationSheetId' in x else None)
    df_expandir_aprop['rate'] = df_expandir_aprop['buldingCosts'].apply(lambda x: x['rate'] if isinstance(x, dict) and 'rate' in x else None)
else:
    df_expandir_aprop['buildingId'] = None
    df_expandir_aprop['buildingName'] = None
    df_expandir_aprop['buildingUnitId'] = None
    df_expandir_aprop['costEstimationSheetId'] = None
    df_expandir_aprop['rate'] = None

# Preencher os valores `None` com uma string vazia
df_expandir_aprop.fillna('', inplace=True)

# Manter todas as colunas exceto `financialCategories` e `buldingCosts`
df_final_pandas_aprop = df_expandir_aprop.drop(columns=['financialCategories', 'buldingCosts'])

# Filtrar empresas
company_ids_aprop = [66] # Filtrar todas as empresas que deseja retornar os dados
df_final_pandas_aprop = df_final_pandas_aprop[df_final_pandas_aprop['companyId'].isin(company_ids_aprop)]

# Obter a data e hora atual
data_atualizacao = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# Adicionar a coluna 'DataHoraAtualizacao' com o valor da data e hora atual
df_final_pandas['DataHoraAtualizacao'] = data_atualizacao
df_final_pandas_aprop['DataHoraAtualizacao'] = data_atualizacao

# Caminho para salvar os arquivos JSON na pasta 'db'
output_file_name = os.path.join(repo_dir, 'db', 'db_mov_bancarias.json')
output_file_name_aprop = os.path.join(repo_dir, 'db', 'db_mov_bancarias_aprop.json')

# Salvar o DataFrame em um novo arquivo JSON
df_final_pandas.to_json(output_file_name, orient='records', lines=False, force_ascii=False, indent=4)
df_final_pandas_aprop.to_json(output_file_name_aprop, orient='records', lines=False, force_ascii=False, indent=4)

print("DataFrame salvo com sucesso!")