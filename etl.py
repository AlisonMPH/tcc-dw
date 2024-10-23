import os
import json
import requests
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import logging
import zipfile
import chardet

# Diretório para logs
log_dir = r'C:\\git\\tcc-dw'
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Caminho completo do arquivo de log
log_file = os.path.join(log_dir, 'etl.log')

# Configurar o arquivo de log com data e hora no início de cada linha
logging.basicConfig(filename=log_file, 
                    level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s', 
                    datefmt='%Y-%m-%d %H:%M:%S')

# Função para carregar as configurações do banco e do diretório de imagens a partir do arquivo JSON
def load_config():
    # Definir o caminho completo para o arquivo config.json
    config_path = os.path.join(log_dir, 'config.json')
    
    # Abrir o arquivo config.json
    with open(config_path, 'r') as config_file:
        config = json.load(config_file)
    
    # Verifica qual ambiente está sendo usado (dev ou prod)
    environment = config['environment']
    return config[environment]  # Retorna as configurações do ambiente selecionado

# Configurações
config = load_config()
base_url = config['base_url']
download_dir = config['download_dir']
database_url = config['database_url']

def criar_diretorio(diretorio):
    if not os.path.exists(diretorio):
        os.makedirs(diretorio)

def extrair_dados(inicio_ano, fim_ano):
    urls = []
    for ano in range(inicio_ano, fim_ano + 1):
        for mes in range(1, 13):
            mes_str = f"{mes:02d}"  # Formata o mês com dois dígitos
            url = f"{base_url}{ano}{mes_str}"
            urls.append(url)
    return urls

def baixar_dados(urls, download_dir):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    }
    
    for url in urls:
        try:
            # Define o nome do arquivo ZIP a ser salvo
            nome_arquivo_zip = os.path.join(download_dir, url.split("/")[-1])

            # Extrai ano e mês do nome do arquivo ZIP para criar o nome final
            nome_base = os.path.splitext(os.path.basename(nome_arquivo_zip))[0]  # remove a extensão .zip
            ano_mes = ''.join([char for char in nome_base if char.isdigit()])  # extrai os números (ano e mês)
            nome_final = f"{ano_mes}_Despesas.csv"
            
            # Verifica se o arquivo CSV já existe
            caminho_final = os.path.join(download_dir, nome_final)
            if os.path.exists(caminho_final):
                logging.info(f"O arquivo CSV {nome_final} já existe. Pulando o download.")
                continue 
            # Verifica se o arquivo já foi baixado
            if os.path.exists(nome_arquivo_zip):
                logging.info(f"Arquivo já existe: {nome_arquivo_zip}. Pulando download.")
                continue

            # Faz o download do arquivo
            response = requests.get(url, headers=headers, stream=True)
            response.raise_for_status()  # Levanta um erro para status não 200

            # Salva o arquivo ZIP
            with open(nome_arquivo_zip, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192): 
                    f.write(chunk)

            logging.info(f"Arquivo ZIP salvo: {nome_arquivo_zip}")

            # Extrai o arquivo ZIP
            with zipfile.ZipFile(nome_arquivo_zip, 'r') as zip_ref:
                zip_ref.extractall(download_dir)
            logging.info(f"Arquivo(s) extraído(s) para: {download_dir}")

            # Remove o arquivo ZIP após a extração
            os.remove(nome_arquivo_zip)
            logging.info(f"Arquivo ZIP removido: {nome_arquivo_zip}")

        except Exception as e:
            logging.error(f"Erro ao baixar ou extrair: {url} - Erro: {e}")

def detectar_codificacao(arquivo):
    with open(arquivo, 'rb') as f:
        resultado = chardet.detect(f.read())
    return resultado['encoding']

def transformar_dados(download_dir):
    arquivos = os.listdir(download_dir)
    dataframes = []
    for arquivo in arquivos:
        if arquivo.endswith('.csv'):
            caminho_arquivo = os.path.join(download_dir, arquivo)
            try:
                # Detectar a codificação do arquivo
                #codificacao = detectar_codificacao(caminho_arquivo)
                #df = pd.read_csv(caminho_arquivo, encoding=codificacao)
                #df = pd.read_csv(os.path.join(download_dir, arquivo), encoding='ISO-8859-1')
                df = pd.read_csv(os.path.join(download_dir, arquivo), encoding='ISO-8859-1', delimiter=';', on_bad_lines='skip')
                dataframes.append(df)
            except Exception as e:
                logging.error(f"Erro ao ler o arquivo {arquivo}: {e}")
                continue

    return dataframes

def carregar_dados(df):
    engine = create_engine(database_url)  # Conexão com PostgreSQL
    df.to_sql('despesas', con=engine, if_exists='replace', index=False)
    logging.info("Dados carregados com sucesso no banco de dados.")

def etl(inicio_ano=2022, fim_ano=datetime.now().year):
    criar_diretorio(download_dir)
    urls = extrair_dados(inicio_ano, fim_ano)
    baixar_dados(urls, download_dir)
    df = transformar_dados(download_dir)
    carregar_dados(df)

# Executa o ETL
if __name__ == "__main__":
    etl()
