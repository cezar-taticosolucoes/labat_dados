# Importar Bibliotecas
import pandas as pd
import json
from IPython.display import display
import os

# Definir o caminho correto para o arquivo JSON na pasta 'files'
# Obter o caminho absoluto do diretório raiz do repositório (AIKON_BANCO_DADOS)
repo_dir = os.path.dirname(os.path.abspath(''))  # 'file' removido, usará o diretório de execução atual
file_name = os.path.join(repo_dir, 'files', 'insumos_bravaaikon.json')

# Carregar o JSON e transformar em DataFrame
data = pd.read_json(file_name)  # Usar pd.read_json ao invés de load_json
df = pd.DataFrame(data)

# Remover colunas especificadas
colunas_remover = ["priceCategory", "synonym", "taxClassification", "productTax", "isActive", "trademarkId", "trademarkDescription", "minimumStock", "maximumStock", "hasServiceFeature", "deliveryInterval", "movementUnits", "notes", "disbursements", "buildingAppropriations", "remainingDisbursement"]
df_colunas_remover = df.drop(columns = colunas_remover)

# Remover colunas especificadas
colunas_remover = ["priceCategory", "synonym", "taxClassification", "productTax", "isActive", "trademarkId", "trademarkDescription", "minimumStock", "maximumStock", "hasServiceFeature", "deliveryInterval", "movementUnits", "notes", "disbursements", "buildingAppropriations", "remainingDisbursement"]
df_colunas_remover = df.drop(columns = colunas_remover)

# Expandir coluna 'installments' do DataFrame
df_expandir_installments = df_colunas_remover.explode('installments')

# Verificar se a coluna 'installments' existe e contém dados
if 'installments' in df_expandir_installments.columns:
    df_expandir_installments['disbursementDays'] = df_expandir_installments['installments'].apply(lambda x: x['disbursementDays'] if pd.notnull(x) and 'disbursementDays' in x else None)
    df_expandir_installments['disbursementPercent'] = df_expandir_installments['installments'].apply(lambda x: x['disbursementDays'] if pd.notnull(x) and 'disbursementPercent' in x else None)
else:
    df_expandir_installments['disbursementDays'] = None
    df_expandir_installments['disbursementPercent'] = None

# Preencher os valores `None` com uma string vazia
df_expandir_installments['disbursementDays'] = df_expandir_installments['disbursementDays'].fillna('')
df_expandir_installments['disbursementPercent'] = df_expandir_installments['disbursementPercent'].fillna('')

# Expandir coluna 'buildingCostEstimationItems' do DataFrame
df_expandir_buildingCostEstimationItems = df_expandir_installments.explode('buildingCostEstimationItems')

# Verificar se a coluna 'buildingCostEstimationItems' existe e contém dados
if 'buildingCostEstimationItems' in df_expandir_buildingCostEstimationItems.columns:
    df_expandir_buildingCostEstimationItems['buildingUnitId'] = df_expandir_buildingCostEstimationItems['buildingCostEstimationItems'].apply(lambda x: x['buildingUnitId'] if pd.notnull(x) and 'buildingUnitId' in x else None)
    df_expandir_buildingCostEstimationItems['wbsCode'] = df_expandir_buildingCostEstimationItems['buildingCostEstimationItems'].apply(lambda x: x['wbsCode'] if pd.notnull(x) and 'wbsCode' in x else None)
    df_expandir_buildingCostEstimationItems['sheetItemId'] = df_expandir_buildingCostEstimationItems['buildingCostEstimationItems'].apply(lambda x: x['sheetItemId'] if pd.notnull(x) and 'sheetItemId' in x else None)
    df_expandir_buildingCostEstimationItems['totalPrice'] = df_expandir_buildingCostEstimationItems['buildingCostEstimationItems'].apply(lambda x: x['totalPrice'] if pd.notnull(x) and 'totalPrice' in x else None)
    df_expandir_buildingCostEstimationItems['quantity'] = df_expandir_buildingCostEstimationItems['buildingCostEstimationItems'].apply(lambda x: x['quantity'] if pd.notnull(x) and 'quantity' in x else None)
else:
    df_expandir_buildingCostEstimationItems['buildingUnitId'] = None
    df_expandir_buildingCostEstimationItems['wbsCode'] = None
    df_expandir_buildingCostEstimationItems['sheetItemId'] = None
    df_expandir_buildingCostEstimationItems['totalPrice'] = None
    df_expandir_buildingCostEstimationItems['quantity'] = None

# Preencher os valores `None` com uma string vazia
df_expandir_buildingCostEstimationItems['buildingUnitId'] = df_expandir_buildingCostEstimationItems['buildingUnitId'].fillna('')
df_expandir_buildingCostEstimationItems['wbsCode'] = df_expandir_buildingCostEstimationItems['wbsCode'].fillna('')
df_expandir_buildingCostEstimationItems['sheetItemId'] = df_expandir_buildingCostEstimationItems['sheetItemId'].fillna('')
df_expandir_buildingCostEstimationItems['totalPrice'] = df_expandir_buildingCostEstimationItems['totalPrice'].fillna('')
df_expandir_buildingCostEstimationItems['quantity'] = df_expandir_buildingCostEstimationItems['quantity'].fillna('')

# Manter todas as colunas exceto as especificadas
df_final_pandas = df_expandir_buildingCostEstimationItems.drop(columns= ['installments', 'buildingCostEstimationItems'])

# Caminho para salvar os arquivos JSON na pasta 'db'
output_file_name_orc = os.path.join(repo_dir, 'db', 'db_insumos_orcados_thebridge.json')

# Salvar o DataFrame em um novo arquivo JSON
df_final_pandas.to_json(output_file_name_orc, orient='records', lines=False, force_ascii=False, indent=4)

print("Arquivos salvos com sucesso!")