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
# Obter o caminho absoluto do diretório raiz do repositório
repo_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
file_name = os.path.join(repo_dir, 'files', 'pagamentos.json')

# Carregar o JSON e transformar em DataFrame
data = load_json(file_name)
df = pd.DataFrame(data)

# Filtrar projetos
project_ids = [1] # Filtrar todas os projetos que deseja retornar os dados
df = df[df['projectId'].isin(project_ids)]

# Filtrar empresas
company_ids = [66] # Filtrar todas as empresas que deseja retornar os dados
df = df[df['companyId'].isin(company_ids)]

# DataFrame Colunas Payments
df_payments = df[['companyId', 'projectId', 'billId', 'installmentId', 'documentIdentificationId', 'payments']]

df_expanded = df_payments.explode('payments')

df_expanded_payments = pd.json_normalize(df_expanded['payments'])

df_expanded.reset_index(drop=True, inplace=True)
df_expanded_payments.reset_index(drop=True, inplace=True)

df_final_payments = pd.concat([df_expanded.drop(columns=['payments']), df_expanded_payments], axis=1)

df_cleaned = df_final_payments.dropna(subset=['operationTypeId'])

colunas_remover = [
    "calculationDate", "paymentAuthentication", "sequencialNumber", "correctedNetAmount", "bankMovements"
]

df_final_payments = df_cleaned.drop(columns=colunas_remover)

#DataFrame Pagamentos Original
columns_remove = ['businessAreaId', 'businessAreaName', 'groupCompanyId', 'groupCompanyName',
    'holdingId', 'holdingName', 'subsidiaryId', 'subsidiaryName', 'businessTypeId', 'businessTypeName',
    'indexerId', 'indexerName', 'issueDate', 'installmentBaseDate', 'authorizationStatus', 'billDate', 
    'registeredUserId', 'registeredBy', 'registeredDate', 'departamentsCosts',
    'buildingsCosts', 'payments', 'authorizations'
    ]

df_columns = df.drop(columns=columns_remove)

# Explodir a coluna `paymentsCategories` no DataFrame pandas
df_expandir_categorias = df_columns.explode('paymentsCategories')

# Verificar se a coluna `paymentsCategories` existe e contém dados
if 'paymentsCategories' in df_expandir_categorias.columns:
    df_expandir_categorias['costCenterId'] = df_expandir_categorias['paymentsCategories'].apply(lambda x: x['costCenterId'] if pd.notnull(x) and 'costCenterId' in x else None)
    df_expandir_categorias['costCenterName'] = df_expandir_categorias['paymentsCategories'].apply(lambda x: x['costCenterName'] if pd.notnull(x) and 'costCenterName' in x else None)
    df_expandir_categorias['financialCategoryId'] = df_expandir_categorias['paymentsCategories'].apply(lambda x: x['financialCategoryId'] if pd.notnull(x) and 'financialCategoryId' in x else None)
    df_expandir_categorias['financialCategoryName'] = df_expandir_categorias['paymentsCategories'].apply(lambda x: x['financialCategoryName'] if pd.notnull(x) and 'financialCategoryName' in x else None)
    df_expandir_categorias['financialCategoryRate'] = df_expandir_categorias['paymentsCategories'].apply(lambda x: x['financialCategoryRate'] if pd.notnull(x) and 'financialCategoryRate' in x else None)
else:
    df_expandir_categorias['costCenterId'] = None
    df_expandir_categorias['costCenterName'] = None
    df_expandir_categorias['financialCategoryId'] = None
    df_expandir_categorias['financialCategoryName'] = None
    df_expandir_categorias['financialCategoryRate'] = None

# Preencher os valores `None` com uma string vazia
df_expandir_categorias['costCenterId'] = df_expandir_categorias['costCenterId'].fillna('')
df_expandir_categorias['costCenterName'] = df_expandir_categorias['costCenterName'].fillna('')
df_expandir_categorias['financialCategoryId'] = df_expandir_categorias['financialCategoryId'].fillna('')
df_expandir_categorias['financialCategoryName'] = df_expandir_categorias['financialCategoryName'].fillna('')
df_expandir_categorias['financialCategoryRate'] = df_expandir_categorias['financialCategoryRate'].fillna('')

# Confirmar se todas as colunas para a junção existem em ambos os DataFrames
common_columns = ['companyId', 'projectId', 'billId', 'installmentId', 'documentIdentificationId']
for col in common_columns:
    if col not in df_expandir_categorias.columns:
        print(f"Coluna {col} não encontrada no df_categorias")
    if col not in df_final_payments.columns:
        print(f"Coluna {col} não encontrada no df_final_receipts")

# Realizar a junção (merge) dos dois DataFrames
df_resultado_pagamentos = pd.merge(
    df_expandir_categorias, # DataFrame original,
    df_final_payments[[
        'companyId', 'projectId', 'billId', 'installmentId',
        'documentIdentificationId', 'operationTypeId', 'operationTypeName',
        'grossAmount', 'monetaryCorrectionAmount', 'interestAmount',
        'fineAmount', 'discountAmount', 'taxAmount', 'netAmount',
        'paymentDate'
    ]], # DataFrame com as colunas que desejamos levar para o DataFrame orginal
    on= ['companyId', 'projectId', 'billId', 'installmentId', 'documentIdentificationId'], # Chaves comuns
    how='left'  # Mantemos todos os dados do df_categorias, mesmo que não haja correspondência no df_final_receipts
)

print("Colunas mescladas com sucesso!!!")

# Dicionário com os tipos de dados desejados para cada coluna
dtype_dict_pagamentos = {
    'companyId': 'int64',
    'companyName': 'str',
    'projectId': 'int64',
    'projectName': 'str',
    'creditorId': 'str',
    'creditorName': 'str',
    'billId': 'str',
    'installmentId': 'str',
    'documentIdentificationId': 'str',
    'documentIdentificationName': 'str',
    'documentNumber': 'str',
    'forecastDocument': 'str',
    'consistencyStatus': 'str',
    'originId': 'str',
    'originalAmount': 'float64',
    'discountAmount_x': 'float64',
    'taxAmount_x': 'float64',
    'dueDate': 'datetime64[ns]',
    'balanceAmount': 'float64',
    'correctedBalanceAmount': 'float64',
    'paymentsCategories': 'str',
    'operationTypeId': 'str',
    'operationTypeName': 'str',
    'grossAmount': 'float64',
    'monetaryCorrectionAmount': 'float64',
    'interestAmount': 'float64',
    'fineAmount': 'float64',
    'discountAmount_y': 'float64',
    'taxAmount_y': 'float64',
    'netAmount': 'float64',
    'paymentDate': 'datetime64[ns]',
    'costCenterId': 'int64',
    'costCenterName': 'str',
    'financialCategoryId': 'int64',
    'financialCategoryName': 'str',
    'financialCategoryRate': 'float64'
}

# Aplicar os tipos de dados ao DataFrame
df_final_pagamentos = df_resultado_pagamentos.astype(dtype_dict_pagamentos)
df_final_pagamentos= df_final_pagamentos.drop(columns=['paymentsCategories'])
df_final_pagamentos['financialCategoryRate'] = df_final_pagamentos['financialCategoryRate'] / 100

# Converta a coluna de data para string no formato desejado
df_final_pagamentos['dueDate'] = df_final_pagamentos['dueDate'].dt.strftime('%Y-%m-%d')
df_final_pagamentos['paymentDate'] = df_final_pagamentos['paymentDate'].dt.strftime('%Y-%m-%d')

# DataFrame Apropriações
remove_columns_aprop = [
    "businessAreaId", "businessAreaName", "groupCompanyId", "groupCompanyName",
    "holdingId", "holdingName", "subsidiaryId", "subsidiaryName", "businessTypeId",
    "businessTypeName", "indexerId", "indexerName", "issueDate", "installmentBaseDate",
    "authorizationStatus", "billDate", "registeredUserId", "registeredBy", "registeredDate",
    "departamentsCosts", "authorizations", "paymentsCategories", "payments"
    ]

df_remove_aprop = df.drop(columns=remove_columns_aprop)

# Explodir a coluna `buildingsCosts` no DataFrame pandas
df_expandir_aprop = df_remove_aprop.explode('buildingsCosts')

# Verificar se a coluna `buildingsCosts` existe e contém dados
if 'buildingsCosts' in df_expandir_aprop.columns:
    df_expandir_aprop['buildingId'] = df_expandir_aprop['buildingsCosts'].apply(lambda x: x['buildingId'] if pd.notnull(x) and 'buildingId' in x else None)
    df_expandir_aprop['buildingName'] = df_expandir_aprop['buildingsCosts'].apply(lambda x: x['buildingName'] if pd.notnull(x) and 'buildingName' in x else None)
    df_expandir_aprop['buildingUnitId'] = df_expandir_aprop['buildingsCosts'].apply(lambda x: x['buildingUnitId'] if pd.notnull(x) and 'buildingUnitId' in x else None)
    df_expandir_aprop['costEstimationSheetId'] = df_expandir_aprop['buildingsCosts'].apply(lambda x: x['costEstimationSheetId'] if pd.notnull(x) and 'costEstimationSheetId' in x else None)
    df_expandir_aprop['rate'] = df_expandir_aprop['buildingsCosts'].apply(lambda x: x['rate'] if pd.notnull(x) and 'rate' in x else None)
else:
    df_expandir_aprop['buildingId'] = None
    df_expandir_aprop['buildingName'] = None
    df_expandir_aprop['buildingUnitId'] = None
    df_expandir_aprop['costEstimationSheetId'] = None
    df_expandir_aprop['rate'] = None

# Preencher os valores `None` com uma string vazia
df_expandir_aprop['buildingId'] = df_expandir_aprop['buildingId'].fillna('')
df_expandir_aprop['buildingName'] = df_expandir_aprop['buildingName'].fillna('')
df_expandir_aprop['buildingUnitId'] = df_expandir_aprop['buildingUnitId'].fillna('')
df_expandir_aprop['costEstimationSheetId'] = df_expandir_aprop['costEstimationSheetId'].fillna('')

# Confirmar se todas as colunas para a junção existem em ambos os DataFrames
common_columns = ['companyId', 'projectId', 'billId', 'installmentId', 'documentIdentificationId']
for col in common_columns:
    if col not in df_expandir_aprop.columns:
        print(f"Coluna {col} não encontrada no df_categorias")
    if col not in df_final_payments.columns:
        print(f"Coluna {col} não encontrada no df_final_receipts")

# Realizar a junção (merge) dos dois DataFrames
df_resultado_aprop = pd.merge(
    df_expandir_aprop, # DataFrame original,
    df_final_payments[[
        'companyId', 'projectId', 'billId', 'installmentId',
        'documentIdentificationId', 'operationTypeId', 'operationTypeName',
        'grossAmount', 'monetaryCorrectionAmount', 'interestAmount',
        'fineAmount', 'discountAmount', 'taxAmount', 'netAmount',
        'paymentDate'
    ]], # DataFrame com as colunas que desejamos levar para o DataFrame orginal
    on= ['companyId', 'projectId', 'billId', 'installmentId', 'documentIdentificationId'], # Chaves comuns
    how='left'  # Mantemos todos os dados do df_categorias, mesmo que não haja correspondência no df_final_receipts
)

print("Colunas mescladas com sucesso!!!")

# Dicionário com os tipos de dados desejados para cada coluna
dtype_dict_aprop = {
    'companyId': 'int64',
    'companyName': 'str',
    'projectId': 'int64',
    'projectName': 'str',
    'creditorId': 'int64',
    'creditorName': 'str',
    'billId': 'int64',
    'installmentId': 'int64',
    'documentIdentificationId': 'str',
    'documentIdentificationName': 'str',
    'documentNumber': 'str',
    'forecastDocument': 'str',
    'consistencyStatus': 'str',
    'originId': 'str',
    'originalAmount': 'float64',
    'discountAmount_x': 'float64',
    'taxAmount_x': 'float64',
    'dueDate': 'datetime64[ns]',
    'balanceAmount': 'float64',
    'correctedBalanceAmount': 'float64',
    'buildingsCosts': 'str',
    'operationTypeId': 'str',
    'operationTypeName': 'str',
    'grossAmount': 'float64',
    'monetaryCorrectionAmount': 'float64',
    'interestAmount': 'float64',
    'fineAmount': 'float64',
    'discountAmount_y': 'float64',
    'taxAmount_y': 'float64',
    'netAmount': 'float64',
    'paymentDate': 'datetime64[ns]',
    'buildingId': 'str',
    'buildingName': 'str',
    'buildingUnitId': 'str',
    'costEstimationSheetId': 'str',
    'rate': 'float64'
}

# Aplicar os tipos de dados ao DataFrame
df_final_aprop = df_resultado_aprop.astype(dtype_dict_aprop)
df_final_aprop= df_final_aprop.drop(columns=['buildingsCosts'])
df_final_aprop['rate'] = df_final_aprop['rate']/100

# Converta a coluna de data para string no formato desejado
df_final_aprop['dueDate'] = df_final_aprop['dueDate'].dt.strftime('%Y-%m-%d')
df_final_aprop['paymentDate'] = df_final_aprop['paymentDate'].dt.strftime('%Y-%m-%d')

# Obter a data e hora atual
data_atualizacao = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# Adicionar a coluna 'DataHoraAtualizacao' com o valor da data e hora atual
df_final_pagamentos['DataHoraAtualizacao'] = data_atualizacao
df_final_aprop['DataHoraAtualizacao'] = data_atualizacao

# Caminho para salvar os arquivos JSON na pasta 'db'
output_file_name = os.path.join(repo_dir, 'db', 'db_pagamentos.json')
output_file_name_aprop = os.path.join(repo_dir, 'db', 'db_apropriacoes.json')

# Salvar o DataFrame em um novo arquivo JSON
df_final_pagamentos.to_json(output_file_name, orient='records', lines=False, force_ascii=False, indent=4)
df_final_aprop.to_json(output_file_name_aprop, orient='records', lines=False, force_ascii=False, indent=4)

print("DataFrame salvo com sucesso!")