import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
import json

# Carregar configurações do arquivo config.json
with open('config.json', 'r') as file:
    config = json.load(file)

# Selecionar o ambiente desejado (exemplo: 'dev' ou 'prod')
environment = config['environment']
database_url = config[environment]['database_url']

# Criação da engine de conexão com o banco de dados PostgreSQL
engine = create_engine(database_url)

# Carregar as dimensões e fato do banco de dados
dim_tempo = pd.read_sql('SELECT * FROM "DW".dim_tempo', engine)
dim_orgaosuperior = pd.read_sql('SELECT * FROM "DW".dim_orgaosuperior', engine)
dim_orgaosubordinado = pd.read_sql('SELECT * FROM "DW".dim_orgaosubordinado', engine)
dim_unidadegestora = pd.read_sql('SELECT * FROM "DW".dim_unidadegestora', engine)
dim_modalidadedespesa = pd.read_sql('SELECT * FROM "DW".dim_modalidadedespesa', engine)

# Função para obter os dados com base no filtro
@st.cache_data
def get_data_from_database(ano, orgaosuperior, orgaosubordinado, unidadegestora, modalidade):
    query = f'''
    SELECT fg.*, dt.ano, os.nome_orgaosuperior AS orgao_superior, osub.nome_orgaosubordinado AS orgao_subordinado, 
           ug.nome_unidadegestora AS unidade_gestora, dm.nome_modalidadedespesa AS modalidade_des
    FROM "DW".fato_gastomensal fg
    JOIN "DW".dim_tempo dt ON fg.id_tempo = dt.id_tempo
    JOIN "DW".dim_orgaosuperior os ON fg.cod_orgaosuperior = os.cod_orgaosuperior
    JOIN "DW".dim_orgaosubordinado osub ON fg.cod_orgaosubordinado = osub.cod_orgaosubordinado
    JOIN "DW".dim_unidadegestora ug ON fg.cod_unidadegestora = ug.cod_unidadegestora
    JOIN "DW".dim_modalidadedespesa dm ON fg.cod_modalidadedespesa = dm.cod_modalidadedespesa
    WHERE dt.ano = {ano} 
          AND ((os.nome_orgaosuperior = '{orgaosuperior}' AND '{orgaosuperior}' != 'Todos') OR '{orgaosuperior}' = 'Todos')
          AND ((osub.nome_orgaosubordinado = '{orgaosubordinado}' AND '{orgaosubordinado}' != 'Todos') OR '{orgaosubordinado}' = 'Todos')
          AND ((ug.nome_unidadegestora = '{unidadegestora}' AND '{unidadegestora}' != 'Todos') OR '{unidadegestora}' = 'Todos')
          AND dm.nome_modalidadedespesa = '{modalidade}'
    '''
    return pd.read_sql(query, engine)

# Título da página
st.title('Dashboard de Gastos Federais')

# Filtros Interativos - Organizando os filtros
# Linha 1: Ano e Tipo de Filtro
col1, col2 = st.columns([1, 2])  # Ajustar a largura das colunas para Ano e Tipo de Filtro

with col1:
    ano = st.selectbox('Selecione o Ano', dim_tempo['ano'].unique())

with col2:
    filter_options = ['Órgão Superior', 'Órgão Subordinado', 'Unidade Gestora']
    selected_filter = st.selectbox('Selecione o tipo de filtro', filter_options)

# Linha 2: Filtro de Unidade Gestora ou Órgão Subordinado ou Órgão Superior
col3 = st.columns(1)[0]  # Uma coluna para o filtro correspondente
with col3:
    if selected_filter == 'Unidade Gestora':
        unidadegestora = st.selectbox('Selecione a Unidade Gestora', dim_unidadegestora['nome_unidadegestora'].unique())
        orgaosuperior = 'Todos'
        orgaosubordinado = 'Todos'
    elif selected_filter == 'Órgão Subordinado':
        orgaosubordinado = st.selectbox('Selecione o Órgão Subordinado', dim_orgaosubordinado['nome_orgaosubordinado'].unique())
        unidadegestora = 'Todos'
        orgaosuperior = 'Todos'
    else:
        orgaosuperior = st.selectbox('Selecione o Órgão Superior', dim_orgaosuperior['nome_orgaosuperior'].unique())
        orgaosubordinado = 'Todos'
        unidadegestora = 'Todos'

# Linha 3: Modalidade de Despesa
col4 = st.columns(1)[0]  # Uma coluna para a Modalidade de Despesa
with col4:
    modalidade = st.selectbox('Selecione a Modalidade de Despesa', dim_modalidadedespesa['nome_modalidadedespesa'].unique())

# Carregar dados com cache
df_fato = get_data_from_database(ano, orgaosuperior, orgaosubordinado, unidadegestora, modalidade)

# Agrupar os dados por modalidade e calcular as somas
df_pizza = df_fato.groupby(['modalidade_des']).agg({
    'valor_empenhado': 'sum',
    'valor_liquidado': 'sum',
    'valor_pago': 'sum'
}).reset_index()

# Garantir que as colunas sejam numéricas
df_pizza['valor_empenhado'] = pd.to_numeric(df_pizza['valor_empenhado'], errors='coerce')
df_pizza['valor_liquidado'] = pd.to_numeric(df_pizza['valor_liquidado'], errors='coerce')
df_pizza['valor_pago'] = pd.to_numeric(df_pizza['valor_pago'], errors='coerce')

# Remover ou preencher valores NaN, se necessário
df_pizza_clean = df_pizza.dropna(subset=['valor_empenhado', 'valor_liquidado', 'valor_pago'])

# Verificando se a coluna 'modalidade_des' existe
if 'modalidade_des' in df_pizza_clean.columns:
    # Transformando os dados para formato long (tidy format)
    df_long = df_pizza_clean.melt(id_vars=["modalidade_des"], value_vars=["valor_empenhado", "valor_liquidado", "valor_pago"],
                                  var_name="Categoria", value_name="Valor")
    
    # Plotando o gráfico de barras verticais com Plotly
    fig = px.bar(df_long, x="modalidade_des", y="Valor", color="Categoria", 
                 title="Distribuição dos Gastos por Modalidade", labels={"Valor": "Valor (R$)"})
    
    # Exibir o gráfico
    st.plotly_chart(fig)

# Exibe a tabela com os dados
st.dataframe(df_fato)
