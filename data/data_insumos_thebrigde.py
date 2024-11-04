# Importar Bibliotecas
import pandas as pd
from datetime import datetime
import json
from IPython.display import display
import os

# Definir o caminho correto para o arquivo JSON na pasta 'files'
# Obter o caminho absoluto do diretório raiz do repositório 
repo_dir = os.path.dirname(os.path.abspath(''))  # 'file' removido, usará o diretório de execução atual
file_name = os.path.join(repo_dir, 'files', 'insumos_thebridge.json')

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
    df_expandir_installments['disbursementPercent'] = df_expandir_installments['installments'].apply(lambda x: x['disbursementPercent'] if pd.notnull(x) and 'disbursementPercent' in x else None)
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

# Remover colunas especificadas
colunas_remover_aprop = ["buildingName", "priceCategory", "synonym", "taxClassification", "productTax", "isActive", "trademarkId", "trademarkDescription", "minimumStock", "maximumStock", "estimatedDeliveryTime", "hasServiceFeature", "disbursements", "installments", "deliveryInterval", "notes", "remainingDisbursement", "buildingCostEstimationItems"]
df_colunas_remover_aprop = df.drop(columns=colunas_remover_aprop)

# Expandir coluna 'movementUnits'
df_expandir_movementUnits = df_colunas_remover_aprop.explode('movementUnits')

# Verificar se a coluna movementUnits existe e contém dados
if 'movementUnits' in df_expandir_movementUnits.columns:
    df_expandir_movementUnits['unitOfMeasureSymbol'] = df_expandir_movementUnits['movementUnits'].apply(lambda x: x['unitOfMeasureSymbol'] if pd.notnull(x) and 'unitOfMeasureSymbol' in x else None)
    df_expandir_movementUnits['unitOfMeasureDescription'] = df_expandir_movementUnits['movementUnits'].apply(lambda x: x['unitOfMeasureDescription'] if pd.notnull(x) and 'unitOfMeasureDescription' in x else None)
    df_expandir_movementUnits['conversionFactor'] = df_expandir_movementUnits['movementUnits'].apply(lambda x: x['conversionFactor'] if pd.notnull(x) and 'conversionFactor' in x else None)
    df_expandir_movementUnits['isDefault'] = df_expandir_movementUnits['movementUnits'].apply(lambda x: x['isDefault'] if pd.notnull(x) and 'isDefault' in x else None)
    df_expandir_movementUnits['isActive'] = df_expandir_movementUnits['movementUnits'].apply(lambda x: x['isActive'] if pd.notnull(x) and 'isActive' in x else None)
else:
    df_expandir_movementUnits['unitOfMeasureSymbol'] = None
    df_expandir_movementUnits['unitOfMeasureDescription'] = None
    df_expandir_movementUnits['conversionFactor'] = None
    df_expandir_movementUnits['isDefault'] = None
    df_expandir_movementUnits['isActive'] = None

# Preencher os valores None com uma string vazia
df_expandir_movementUnits['unitOfMeasureSymbol'] = df_expandir_movementUnits['unitOfMeasureSymbol'].fillna('')
df_expandir_movementUnits['unitOfMeasureDescription'] = df_expandir_movementUnits['unitOfMeasureDescription'].fillna('')
df_expandir_movementUnits['conversionFactor'] = df_expandir_movementUnits['conversionFactor'].fillna('')
df_expandir_movementUnits['isDefault'] = df_expandir_movementUnits['isDefault'].fillna('')
df_expandir_movementUnits['isActive'] = df_expandir_movementUnits['isActive'].fillna('')

# Manter todas as colunas exceto as especificadas
df_final_aprop = df_expandir_movementUnits.drop(columns= ['movementUnits'])

# Função para extrair dados de 'attended'
def extract_attended(data_dict):
        # Retorna a lista associada à chave 'attended'
        return data_dict.get('attended', [])

# Aplica a função para extrair dados de 'attended' e cria uma nova coluna
df_final_aprop['attended'] = df_final_aprop['buildingAppropriations'].apply(extract_attended)

# Expande a coluna 'attended' em várias linhas
attended_expanded = df_final_aprop.explode('attended')  

# Cria um DataFrame com os dados expandidos
attended_df = pd.json_normalize(attended_expanded['attended'])

# Adiciona as colunas expandidas de volta ao DataFrame original
# Primeiro, restaura o índice para que os dados possam ser combinados corretamente
attended_expanded = attended_expanded.reset_index(drop=True)
attended_df = attended_df.reset_index(drop=True)

# Faz o merge com o DataFrame original baseado no índice
df_final_aprop_expanded = pd.concat([attended_expanded.drop(columns='attended'), attended_df], axis=1)

# Remover colunas especificadas
remove = [
    "buildingName", "priceCategory", "resourceCode", "category", "resourceGroup", "synonym", "financialCategory", "taxClassification", "productTax", "isActive", "trademarkId", "trademarkDescription", "minimumStock", "maximumStock", "estimatedDeliveryTime", "hasServiceFeature", "deliveryInterval", "installments", "movementUnits", "notes", "buildingCostEstimationItems", "disbursements", "remainingDisbursement"
    ]

df_remove = df.drop(columns = remove)

# Função para extrair dados de 'attended'
def extract_attended(data_dict):
        # Retorna a lista associada à chave 'attended'
        return data_dict.get('attended', [])

# Aplica a função para extrair dados de 'attended' e cria uma nova coluna
df_remove['attended'] = df_remove['buildingAppropriations'].apply(extract_attended)

# Expande a coluna 'attended' em várias linhas
expandir_attended = df_remove.explode('attended')

# Cria um DataFrame com os dados expandidos
expandir_attended_df = pd.json_normalize(expandir_attended['attended'])

# Adiciona as colunas expandidas de volta ao DataFrame original
# Primeiro, restaura o índice para que os dados possam ser combinados corretamente
expandir_attended = expandir_attended.reset_index(drop=True)
expandir_attended_df = expandir_attended_df.reset_index(drop=True)

# Faz o merge com o DataFrame original baseado no índice
df_insumos = pd.concat([expandir_attended.drop(columns='attended'), expandir_attended_df], axis=1)

# Remover colunas especificadas
remove = [
    "buildingName", "priceCategory", "resourceCode", "category", "resourceGroup", "synonym", "financialCategory", "taxClassification", "productTax", "isActive", "trademarkId", "trademarkDescription", "minimumStock", "maximumStock", "estimatedDeliveryTime", "hasServiceFeature", "deliveryInterval", "installments", "movementUnits", "notes", "buildingCostEstimationItems", "disbursements", "remainingDisbursement"
    ]

df_remove = df.drop(columns = remove)

# Função para extrair dados de 'attended'
def extract_attended(data_dict):
        # Retorna a lista associada à chave 'attended'
        return data_dict.get('attended', [])

# Aplica a função para extrair dados de 'attended' e cria uma nova coluna
df_remove['attended'] = df_remove['buildingAppropriations'].apply(extract_attended)

# Expande a coluna 'attended' em várias linhas
expandir_attended = df_remove.explode('attended')

# Cria um DataFrame com os dados expandidos
expandir_attended_df = pd.json_normalize(expandir_attended['attended'])

# Adiciona as colunas expandidas de volta ao DataFrame original
# Primeiro, restaura o índice para que os dados possam ser combinados corretamente
expandir_attended = expandir_attended.reset_index(drop=True)
expandir_attended_df = expandir_attended_df.reset_index(drop=True)

# Faz o merge com o DataFrame original baseado no índice
df_insumos = pd.concat([expandir_attended.drop(columns='attended'), expandir_attended_df], axis=1)

columns_remove = [
    "buildingAppropriations", "documentLabel", "costEstimationItemReference", "costEstimationItemDescription", "buildingUnitId", "buildingUnitName", "percentage", "movementDate"
    ]

df_insumos = df_insumos.drop(columns=columns_remove)

# Criando a coluna com o preço unitário apropriado
df_insumos['custoUnitAprop'] = df_insumos['value'] / df_insumos['quantity']

# Converter a coluna 'date' para datetime, garantindo que valores inválidos sejam tratados
df_insumos['date'] = pd.to_datetime(df_insumos['date'], errors='coerce')

# Preencher valores nulos de 'detailId' com um valor padrão para garantir que não sejam ignorados
df_insumos['detailId'] = df_insumos['detailId'].fillna('desconhecido')

# Remover linhas onde 'date' é NaN, pois não podemos determinar a data mais recente sem essa informação
df_insumos = df_insumos.dropna(subset=['date'])

# Verificar se o DataFrame não está vazio após remover as linhas com 'NaN'
if not df_insumos.empty:
    # Para cada combinação de 'id' e 'detailId', pegar o índice da linha com a data mais recente
    idx = df_insumos.groupby(['id', 'detailId'])['date'].idxmax()

    # Usar o índice para filtrar o DataFrame e obter o valor mais recente de cada grupo
    df_insumos_final = df_insumos.loc[idx].reset_index(drop=True)

else:
    print("O DataFrame está vazio após remover as linhas com valores nulos na coluna 'movementDate'.")

# Converta a coluna de data para string no formato desejado
df_insumos_final['date'] = df_insumos_final['date'].dt.strftime('%Y-%m-%d')

# Obter a data e hora atual
data_atualizacao = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# Adicionar a coluna 'DataHoraAtualizacao' com o valor da data e hora atual
df_final_pandas['DataHoraAtualizacao'] = data_atualizacao
df_final_aprop_expanded['DataHoraAtualizacao'] = data_atualizacao
df_insumos_final['DataHoraAtualizacao'] = data_atualizacao

# Caminho para salvar os arquivos JSON na pasta 'db'
output_file_name_orc = os.path.join(repo_dir, 'db', 'db_insumos_orcados_thebridge.json')
output_file_name_aprop = os.path.join(repo_dir, 'db', 'db_insumos_apropriados_thebridge.json')
output_file_name = os.path.join(repo_dir, 'db', 'db_insumos_thebridge.json')

# Salvar o DataFrame em um novo arquivo JSON
df_final_pandas.to_json(output_file_name_orc, orient='records', lines=False, force_ascii=False, indent=4)
df_final_aprop_expanded.to_json(output_file_name_aprop, orient='records', lines=False, force_ascii=False, indent=4)
df_insumos_final.to_json(output_file_name, orient='records', lines=False, force_ascii=False, indent=4)

print("Arquivos salvos com sucesso!")