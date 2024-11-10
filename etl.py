import os
import json
import logging
import zipfile
#import chardet
import requests
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime


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

#def detectar_codificacao(arquivo):
#    with open(arquivo, 'rb') as f:
#        resultado = chardet.detect(f.read())
#    return resultado['encoding']

def transformar_dados(download_dir):
    arquivos = os.listdir(download_dir)
    dataframes = []
    for arquivo in arquivos:
        if arquivo.endswith('.csv'):
            try:
                df = pd.read_csv(os.path.join(download_dir, arquivo), encoding='ISO-8859-1', delimiter=';', on_bad_lines='skip')
                dataframes.append(df)
                logging.info(f"Arquivo lido: {arquivo}")
            except Exception as e:
                logging.error(f"Erro ao ler o arquivo {arquivo}: {e}")
                continue
    
    # Concatenar todos os dataframes em um único dataframe
    if dataframes:
        df_concatenado = pd.concat(dataframes, ignore_index=True)
        logging.info(f"Concatenado {len(dataframes)} dataframes em um único dataframe.")
    else:
        df_concatenado = pd.DataFrame()  # Retorna um dataframe vazio caso nenhum arquivo seja lido
    
    return df_concatenado

def inserir_dim_tempo(df, database_url, tabela_destino="dim_tempo", schema="DW"):
    # Remover duplicatas na coluna "Ano e mês do lançamento" antes de separar
    df_dp = df[['Ano e mês do lançamento']].drop_duplicates()

    # Separar "Ano e mês do lançamento" em colunas de ano e mês
    df_dp[['ano', 'mes']] = df_dp['Ano e mês do lançamento'].str.split('/', expand=True)
    df_dp['ano'] = df_dp['ano'].astype(int)
    df_dp['mes'] = df_dp['mes'].astype(int)

    # Criar conexão com o banco de dados
    engine = create_engine(database_url)

    # Obter os registros existentes no banco de dados
    with engine.connect() as conn:
        existing_records = conn.execute(
            text(f'SELECT ano, mes FROM "{schema}".{tabela_destino}')
        ).fetchall()

    # Criar um DataFrame para registros existentes
    existing_df = pd.DataFrame(existing_records, columns=['ano', 'mes'])

    # Remover duplicatas locais já presentes no banco
    df_to_insert = df_dp.merge(existing_df, on=['ano', 'mes'], how='left', indicator=True)
    df_to_insert = df_to_insert[df_to_insert['_merge'] == 'left_only'].drop(columns=['_merge', 'Ano e mês do lançamento'])

    if df_to_insert.empty:
        logging.info("Nenhum novo registro para inserir")
        return

    # Preparar a query de inserção
    query = text(f'INSERT INTO "{schema}".{tabela_destino} (ano, mes) VALUES (:ano, :mes)')

    # Executar a inserção em lote dentro de uma transação
    with engine.begin() as conn:  # Transação gerenciada pelo SQLAlchemy
        try:
            # Converter os valores para tipos nativos do Python antes de inserir
            conn.execute(query, [{'ano': int(r['ano']), 'mes': int(r['mes'])} for _, r in df_to_insert.iterrows()])
            logging.info(f"{len(df_to_insert)} novos registros inseridos na tabela {schema + '.' + tabela_destino}.")
        except Exception as e:
            print(f"Erro ao inserir dados: {e}")
            logging.error(f"Erro ao inserir dados: {e}")
            
def inserir_dim(df, database_url, tabela_destino, schema='DW', cod=None, nome=None, _cod=None, _nome=None):
    # Renomear colunas para coincidir com os placeholders na consulta SQL
    df = df.rename(columns={
        cod: _cod,
        nome: _nome
    })

    # Remover duplicatas com base na coluna `_cod`
    df_dp = df.drop_duplicates(subset=[_cod])

    # Criar conexão com o banco de dados
    engine = create_engine(database_url)

    # Obter os registros existentes no banco de dados
    with engine.connect() as conn:
        existing_records = conn.execute(
            text(f"SELECT {_cod} FROM \"{schema}\".{tabela_destino}")
        ).fetchall()

    # Criar um conjunto com os registros existentes
    existing_ids = {row[0] for row in existing_records}

    # Filtrar o DataFrame para remover registros já existentes
    registros_filtrados = df_dp[~df_dp[_cod].isin(existing_ids)]

    if not registros_filtrados.empty:
        # Transformar os dados filtrados do DataFrame em uma lista de dicionários
        registros = registros_filtrados[[_cod, _nome]].to_dict(orient="records")

        # Especificar a query SQL para inserção
        query = text(f"""
            INSERT INTO "{schema}".{tabela_destino} ("{_cod}", "{_nome}")
            VALUES (:cod, :nome)
        """)

        # Executar a inserção em lote dentro de uma transação
        with engine.begin() as conn:
            try:
                conn.execute(query, [{'cod': r[_cod], 'nome': r[_nome]} for r in registros])
                print(f"{len(registros)} dados inseridos com sucesso!")
                logging.info(f"{len(registros)} novos registros inseridos na tabela {schema + '.' + tabela_destino}.")
            except Exception as e:
                print(f"Erro ao inserir dados: {e}")
                logging.error(f"Erro ao inserir dados: {e}")
    else:
        print("Nenhum novo registro para inserir.")
        logging.info("Nenhum novo registro para inserir.")
           
def inserir_fato(df, database_url, tabela_destino, schema='DW', cod_sp=None, cod_sb=None, cod_gs=None,
                cod_ed=None,cod_md=None, _cod_sp=None, _cod_sb=None, _cod_gs=None, _cod_ed=None, _cod_md=None, 
                 vl_empenhado=None, vl_liquidado=None, vl_pago=None, 
                 vl_rp_inscrito=None, vl_rp_cancelado=None, vl_rp_pago=None, 
                 _vl_empenhado=None, _vl_liquidado=None, _vl_pago=None, 
                 _vl_rp_inscrito=None, _vl_rp_cancelado=None, _vl_rp_pago=None):
    
    # Criar conexão com o banco de dados
    engine = create_engine(database_url)

    # Separar "Ano e mês do lançamento" em colunas de ano e mês
    df[['ano', 'mes']] = df['Ano e mês do lançamento'].str.split('/', expand=True)
    df['ano'] = df['ano'].astype(int)
    df['mes'] = df['mes'].astype(int)

     # Obter os IDs de ano e mês da tabela dim_tempo
    with engine.connect() as conn:
        # Criar uma lista única de pares de ano e mês
        ano_mes_unicos = df[['ano', 'mes']].drop_duplicates()

        # Criar uma condição de consulta dinâmica para todos os anos e meses
        conditions = " OR ".join(
            [f"(ano = {row['ano']} AND mes = {row['mes']})" for index, row in ano_mes_unicos.iterrows()]
        )

        # Montar a consulta para obter os IDs correspondentes a todos os anos e meses
        query = f"""
            SELECT id_tempo, ano, mes 
            FROM "{schema}".dim_tempo 
            WHERE {conditions}
        """
        
        resultado = conn.execute(text(query)).fetchall()

        # Criar um dicionário para mapear os pares de ano e mês aos seus IDs
        ano_mes_ids = {(row[1], row[2]): row[0] for row in resultado}  # Mudança aqui para índices

    # Mapear os IDs de ano e mês de volta ao DataFrame
    df['id_tempo'] = df.apply(lambda row: ano_mes_ids.get((row['ano'], row['mes'])), axis=1)

    # Filtrar registros com IDs de tempo válidos
    registros_filtrados = df[~df['id_tempo'].isnull()]

    if not registros_filtrados.empty:
        # Verifique se todas as colunas necessárias estão presentes
        required_columns = [cod_sp, cod_sb, cod_gs, cod_ed, cod_md, 'id_tempo', 
                            vl_empenhado, vl_liquidado, vl_pago, 
                            vl_rp_inscrito, vl_rp_cancelado, vl_rp_pago]
        
        missing_columns = [col for col in required_columns if col not in registros_filtrados.columns]

        if missing_columns:
            print(f"Colunas ausentes: {missing_columns}")
            logging.error(f"Colunas ausentes: {missing_columns} ")
            return 

        # Acesso seguro às colunas
        registros = registros_filtrados[required_columns].to_dict(orient="records")

        # Especificar a query SQL para inserção
        query_insert = text(f"""
            INSERT INTO "{schema}".{tabela_destino} ("{_cod_sp}", "{_cod_sb}", "{_cod_gs}", "{_cod_ed}", "{_cod_md}", "id_tempo", 
            "{_vl_empenhado}", "{_vl_liquidado}", "{_vl_pago}", 
            "{_vl_rp_inscrito}", "{_vl_rp_cancelado}", "{_vl_rp_pago}")
            VALUES (:cod_sp, :cod_sb, :cod_gs, :cod_ed, :cod_md, :id_tempo, :vl_empenhado, :vl_liquidado, :vl_pago, 
            :vl_rp_inscrito, :vl_rp_cancelado, :vl_rp_pago)
        """)

        # Preparar os registros para inserção em bloco
        registros_para_inserir = []
        for registro in registros:
            try:
                # Mapear os valores do registro para os parâmetros da query
                registro_map = {
                    'cod_sp': registro['Código Órgão Superior'],  # Ajuste para o nome correto da chave
                    'cod_sb': registro['Código Órgão Subordinado'],
                    'cod_gs': registro['Código Unidade Gestora'],
                    'cod_ed': registro['Código Elemento de Despesa'], 
                    'cod_md': registro['Código Modalidade da Despesa'], 
                    'id_tempo': registro['id_tempo'],
                    'vl_empenhado': registro['Valor Empenhado (R$)'].replace(',', '.'),  # Substituir vírgula por ponto
                    'vl_liquidado': registro['Valor Liquidado (R$)'].replace(',', '.'),
                    'vl_pago': registro['Valor Pago (R$)'].replace(',', '.'),
                    'vl_rp_inscrito': registro['Valor Restos a Pagar Inscritos (R$)'].replace(',', '.'),
                    'vl_rp_cancelado': registro['Valor Restos a Pagar Cancelado (R$)'].replace(',', '.'),
                    'vl_rp_pago': registro['Valor Restos a Pagar Pagos (R$)'].replace(',', '.')
                }

                # Adicionar o registro mapeado à lista para inserção em bloco
                registros_para_inserir.append(registro_map)

            except Exception as e:
                print(f"Erro ao mapear registro: {e}. Registro: {registro}")
                logging.error("Error ao mapear registro: {e}. Registro: {registro}")
                continue  # Continue com o próximo registro

        # Executar a inserção em lote dentro de uma transação
        with engine.begin() as conn:
            try:
                # Log dos dados que estão sendo inseridos
                print(f"Inserindo {len(registros_para_inserir)} registros em bloco.")
                logging.info(f"Inserindo {len(registros_para_inserir)} registros em bloco.")
                
                # Executar a inserção em bloco
                conn.execute(query_insert, registros_para_inserir)

            except Exception as e:
                print(f"Erro ao inserir dados em bloco: {e}")
                logging.error(f"Erro ao inserir dados em bloco: {e}")

    else:
        print("Nenhum novo registro para inserir.")
        logging.info("Nenhum novo registro para inserir.")

def tratar_registros(df):
    # Converte os códigos e valores para serem >= 0
    codigos = ['Código Órgão Superior', 'Código Órgão Subordinado', 'Código Unidade Gestora','Código Modalidade da Despesa','Código Elemento de Despesa']

    # Aplica tratamento aos códigos
    for codigo in codigos:
        if codigo in df.columns:
            df[codigo] = pd.to_numeric(df[codigo], errors='coerce')  # Converte para numérico, substitui erros por NaN
            df[codigo] = df[codigo].apply(lambda x: max(x, 0) if pd.notnull(x) else 0)  # Garantir que sejam >= 0

    return df

def filtrar_dados_novas_datas(df, database_url, schema=None, tabela_fatos=None, tabela_dim=None):
    engine = create_engine(database_url)
    # Obter os id_tempo existentes na tabela fato_gastos
    with engine.connect() as conn:
        query_fatos = text(f"""
            SELECT DISTINCT id_tempo FROM "{schema}"."{tabela_fatos}"
        """)
        id_tempos_existentes = pd.read_sql(query_fatos, conn)

    # Obter o mapeamento de ano, mes e id_tempo da tabela dim_tempo
    with engine.connect() as conn:
        query_dim = text(f"""
            SELECT id_tempo, ano, mes FROM "{schema}"."{tabela_dim}"
        """)
        dim_tempo_df = pd.read_sql(query_dim, conn)

    # Extrair ano e mês do DataFrame e criar um DataFrame de mapeamento
    df[['ano', 'mes']] = df['Ano e mês do lançamento'].str.split('/', expand=True)
    
    # Converter ano e mês para inteiros
    df['ano'] = df['ano'].astype(int)
    df['mes'] = df['mes'].astype(int)

    # Realizar um merge para mapear os id_tempo com base no ano e mês
    df = df.merge(dim_tempo_df, on=['ano', 'mes'], how='left')

    # Filtrar o DataFrame para manter apenas os registros que não têm id_tempo na tabela fato_gastos
    df_filtrado = df[~df['id_tempo'].isin(id_tempos_existentes['id_tempo'])]
    logging.info(f"DataFrame filtrado: %s" % df['id_tempo'])

    return df_filtrado


def esquema_estrela(inicio_ano=2022, fim_ano=datetime.now().year):
    criar_diretorio(download_dir)
    urls = extrair_dados(inicio_ano, fim_ano)
    baixar_dados(urls, download_dir)
    df = transformar_dados(download_dir)
    df = tratar_registros(df)
    inserir_dim_tempo(df, database_url)
    inserir_dim(df, database_url, 'dim_orgaosuperior',    schema='DW', cod='Código Órgão Superior',     nome='Nome Órgão Superior',     _cod='cod_orgaosuperior',       _nome='nome_orgaosuperior')
    inserir_dim(df, database_url, 'dim_orgaosubordinado', schema='DW', cod='Código Órgão Subordinado',  nome='Nome Órgão Subordinado',  _cod='cod_orgaosubordinado',    _nome='nome_orgaosubordinado')
    inserir_dim(df, database_url, 'dim_unidadegestora',   schema='DW', cod='Código Unidade Gestora',    nome='Nome Unidade Gestora',    _cod='cod_unidadegestora',      _nome='nome_unidadegestora')
    inserir_dim(df, database_url, 'dim_modalidadedespesa',schema='DW', cod='Código Modalidade da Despesa',    nome='Modalidade da Despesa',    _cod='cod_modalidadedespesa',      _nome='nome_modalidadedespesa')
    inserir_dim(df, database_url, 'dim_elementodespesa', schema='DW', cod='Código Elemento de Despesa',    nome='Nome Elemento de Despesa',    _cod='cod_elementodespesa',      _nome='nome_elementodespesa')
    df_novos_dados = filtrar_dados_novas_datas(df, database_url, 'DW', 'fato_gastomensal', 'dim_tempo')
    if not df_novos_dados.empty:
        inserir_fato(
            df_novos_dados, 
            database_url, 
            tabela_destino='fato_gastomensal', 
            schema='DW', 
            cod_sp='Código Órgão Superior', 
            cod_sb='Código Órgão Subordinado', 
            cod_gs='Código Unidade Gestora', 
            cod_ed='Código Elemento de Despesa', 
            cod_md='Código Modalidade da Despesa', 
            _cod_sp='cod_orgaosuperior', 
            _cod_sb='cod_orgaosubordinado', 
            _cod_gs='cod_unidadegestora', 
            _cod_ed='cod_elementodespesa', 
            _cod_md='cod_modalidadedespesa', 
            vl_empenhado='Valor Empenhado (R$)', 
            vl_liquidado='Valor Liquidado (R$)', 
            vl_pago='Valor Pago (R$)', 
            vl_rp_inscrito='Valor Restos a Pagar Inscritos (R$)', 
            vl_rp_cancelado='Valor Restos a Pagar Cancelado (R$)', 
            vl_rp_pago='Valor Restos a Pagar Pagos (R$)', 
            _vl_empenhado='valor_empenhado', 
            _vl_liquidado='valor_liquidado', 
            _vl_pago='valor_pago', 
            _vl_rp_inscrito='valor_rp_inscrito', 
            _vl_rp_cancelado='valor_rp_cancelado', 
            _vl_rp_pago='valor_rp_pago')
    else:
        print(f"Nenhum novo registro para inserir na tabela fato.")
        logging.info("Nenhum novo registro para inserir na tabela fato.")

# Executa o ETL
if __name__ == "__main__":
    esquema_estrela()
