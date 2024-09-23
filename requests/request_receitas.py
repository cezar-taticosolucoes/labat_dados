# Importar bibliotecas
import requests
import base64
import pandas as pd
import time
from datetime import datetime
import json
from pandas import json_normalize
import os

# Dados da API Sienge
url_api_base = 'https://api.sienge.com.br/labat/public/api/bulk-data/v1/income'
usuario_api = 'labat-tatico'
senha_api = 'oX2Xm2qO2h1Ie4uS6ETqHdV1fuVnnAus'

# Autenticação
credenciais = base64.b64encode(f"{usuario_api}:{senha_api}".encode('utf-8')).decode('utf-8')

# Parâmetros da consulta
start_date = '2010-01-01'
end_date = datetime.now().strftime('%Y-%m-%d')
selection_type = 'I'

# Montar os parâmetros e cabeçalhos da URL
params = {
    'startDate': start_date,
    'endDate': end_date,
    'selectionType': selection_type
}

headers = {
    'Authorization': f'Basic {credenciais}',
    'Content-Type': 'application/json'
}

# Número máximo de tentativas e delay entre as tentativas
max_retries = 3
retry_delay = 60  # Delay de 60 segundos entre as tentativas

# Fazer a requisição GET na API com tentativas de reconexão
for attempt in range(max_retries):
    try:
        response = requests.get(url_api_base, params=params, headers=headers, timeout=900) # Timeout de 15 minutos
        response.raise_for_status()  # Levanta um erro para status codes 4xx/5xx
        print("Requisição bem-sucedida!")

        break  # Sair do loop se a requisição for bem-sucedida

    except requests.exceptions.RequestException as e:
        print(f"Erro na requisição: {e}")
        if attempt < max_retries - 1:
            print(f"Tentando novamente em {retry_delay} segundos...")
            time.sleep(retry_delay)
        else:
            print("Número máximo de tentativas excedido.")

# Se a requisição for bem-sucedida, processar os dados
data = response.json()['data']

# Função para salvar os dados JSON em um arquivo
def save_json(data, file_name):
    # Obter o caminho absoluto do diretório raiz do repositório
    repo_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    file_path = os.path.join(repo_dir, file_name)

    # Criar o diretório se ele não existir
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    
    print(f"Dados salvos no arquivo {file_path}")

# Executar a requisição e salvar os dados
if data:
    save_json(data, 'files/receitas.json')  # Salva o JSON no arquivo