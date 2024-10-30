
import pandas as pd
import streamlit as st
import pyodbc
import datetime
from dotenv import load_dotenv
import os

#Carrega as credenciais do banco de dados
load_dotenv()

# ----- Querys -----
query_sc6010 = """
DECLARE @DATE_MIN AS DATE
SET @DATE_MIN = DATEFROMPARTS(YEAR(GETDATE()),MONTH(GETDATE()),DAY(GETDATE()))

DECLARE @DATE_MAX AS DATE
SET @DATE_MAX = DATEFROMPARTS(YEAR(GETDATE()),MONTH(GETDATE()),1)

SELECT
	C6_FILIAL,
	C6_ITEM,
	C6_PRODUTO,
	C6_UM,
	C6_QTDVEN,
	C6_PRCVEN,
	C6_VALOR,
	C6_TES,
	C6_CF,
	C6_CLI AS A1_COD,
	C6_LOJA,
	C6_NOTA,
	C6_SERIE,
	CONVERT(DATE,C6_DATFAT,112) AS C6_DATFAT,
	C6_NUM,
	C6_DESCRI,
	C6_PRUNIT,
	LTRIM(RTRIM(C6_LOTECTL)) AS C6_LOTECTL,
	CONVERT(DATE,C6_DTVALID,112) AS C6_DTVALID,
	C5_TIPO,
	C5_VEND1 AS VENDEDOR,
	C5_LIBEROK,
	C6_SUGENTR

FROM
	SC6010

LEFT JOIN SC5010 ON SC6010.C6_FILIAL = SC5010.C5_FILIAL AND SC6010.C6_NUM = SC5010.C5_NUM AND SC6010.C6_CLI = SC5010.C5_CLIENTE



WHERE 
	SC6010.D_E_L_E_T_ = ' '
	AND SC6010.C6_CF IN ('5101','5102','5113','5114','5551','6101','6102','6107','6108','6109','6113','6114','6551','6933')
	AND SC6010.C6_TES NOT IN ('5AY', '5AZ', '5BA', '5BB', '5BC', '6AD', '8LB', '8LC', '8LD', '8LE')
	AND CONVERT(DATE,SC6010.C6_SUGENTR,112) >= @DATE_MIN
 """

query_sa1010 = """
    SELECT 
	A1_COD,
	A1_NOME,
	A1_NREDUZ,
	A1_EST

FROM SA1010
"""

query_sa3010 = """
SELECT
	A3_COD AS VENDEDOR,
	A3_NOME,
	A3_NREDUZ,

	CASE
		WHEN A3_COD = '000004' THEN 'Região A'
		WHEN A3_COD = '000044' THEN 'Região B'
		WHEN A3_COD = '000038' OR A3_COD = '000027' THEN 'Região C'
		WHEN A3_COD = '000025' THEN 'Região Licitação'
	END A3_COD_REGIAO

FROM SA3010
"""

#Cria conexão com banco de dados
def Conexao_bd():
    return {
        'host': os.getenv('DB_HOST'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'database': os.getenv('DB_NAME')
    }

#Transforma a consulta em dataframe
def Criar_tabela(consulta):
    credenciais = Conexao_bd()
    conn = pyodbc.connect(f'Driver={{ODBC Driver 17 for SQL Server}};Server={credenciais["host"]};Database=protheus12_producao;UID={credenciais["user"]};PWD={credenciais["password"]}')
    df = pd.read_sql(consulta, conn)
    return df

#Mesclar tabelas
def Mesclar_tabelas(tabela1, tabela2, coluna_referencia):
    '''
    O nome da coluna referencia precisa ser igual nas duas tabelas
    '''
    df = pd.merge(tabela1, tabela2, how='left', on=coluna_referencia)
    return df

#Cria tabela de pedidos não faturados
def Tabela_pedidos_nao_faturados(tabela_sc6010):
    df = tabela_sc6010[['C6_NUM','C6_QTDVEN','A1_COD','A1_NOME','C6_VALOR','A3_NREDUZ','C5_LIBEROK']] #Seleciona as colunas
    df = df.loc[df['C5_LIBEROK'] == ' '] #Filtra apenas os pedidos não faturados onde C5_LIBEROK esta vazio
    df1 = df.drop_duplicates(subset=['C6_NUM']).reset_index() #Remove os valores duplicados
    df1 = df1.drop(['index','C6_QTDVEN','C6_VALOR','C5_LIBEROK'], axis=1) #Remove colunas
    
    #Cria uma nova tabela com valores e quantidades somadas
    df2 = df.groupby('C6_NUM')[['C6_QTDVEN','C6_VALOR']].sum().reset_index() #Soma os valores das colunas C6_QTDVEN e C6_VALOR
    
    #Remove espaços em branco dos valores
    df2['C6_NUM'] = df2['C6_NUM'].str.strip() 
    for x in df1:
        df1[x] = df1[x].str.strip()
    
    #Mescla as tabelas    
    df_nfat = pd.merge(df1, df2, how='left', on='C6_NUM')
    
    return df_nfat
    
#Cria tabela de pedidos não faturados  
def Tabela_pedidos_faturados(tabela_sc6010):
    df_atual = tabela_sc6010[['C6_NUM','C6_NOTA','C6_QTDVEN','A1_COD','A1_NOME','C6_VALOR','A3_NREDUZ','C6_DATFAT']] #Seleciona as colunas
    df = df_atual.loc[df_atual['C6_DATFAT'] == datetime.date.today() ] #Filtra apenas os pedidos que possuem data de Faturamento
    df1 = df.drop_duplicates(subset=['C6_NOTA']).reset_index() #Remove os valores duplicados
    df1 = df1.drop(['index','C6_VALOR','C6_QTDVEN','C6_DATFAT'], axis=1) #Remove colunas
    
    #Cria uma nova tabela com valores e quantidades somadas
    df2 = df_atual.groupby('C6_NOTA')[['C6_VALOR', 'C6_QTDVEN']].sum().reset_index() #Soma os valores das colunas C6_QTDVEN e C6_VALOR
    
    #Remove espaços em branco do valores
    df2['C6_NOTA'] = df2['C6_NOTA'].str.strip()
    for x in df1:
        df1[x] = df1[x].str.strip()
        
    df_fat = pd.merge(df1,df2,how='left',on='C6_NOTA')
    
    return df_fat
 
#Mascara de moeda para colunas com preço
def formato_moeda(valor):
    return "R${:.2f}".format(valor)

#Calculos de Faturamento, volume e preço médio
def Calcular_faturamento(tabela_sc6010):
    tabela = Tabela_pedidos_faturados(tabela_sc6010)
    soma = tabela['C6_VALOR'].sum()
    total = f'R${soma:_.2f}'
    total = total.replace('.',',').replace('_','.')
    return total
    
def Calcular_volume(tabela_sc6010):
    soma = tabela_sc6010['C6_QTDVEN'].sum()
    volume = f'{int(soma)}'
    
    return volume

def Calcular_preco_medio(tabela_sc6010):
    try:
        faturamento = tabela_sc6010['C6_VALOR'].sum()
        volume = tabela_sc6010['C6_QTDVEN'].sum()
        
        preco_medio = faturamento / volume
        preco_medio = f'R${preco_medio:_.2f}'
        preco_medio = preco_medio.replace('.',',').replace('_','.')
    except:
        preco_medio = 'R$0,00'
        
    return preco_medio

def Renomear_colunas(tabela):

    qtd_cols = tabela.shape[1]
    if qtd_cols == 6:
        tabela.rename(columns={
        'C6_NUM' : 'Pedido',
        'A1_COD' : 'Cód Cliente',
        'A1_NOME' : 'Cliente',
        'A3_NREDUZ' : 'Vendedor',
        'C6_QTDVEN' : 'Quantidade',
        'C6_VALOR' : 'Valor'},
                inplace=True)
    else:
        tabela.rename(columns={
        'C6_NUM' : 'Pedido',
        'C6_NOTA' : 'NF',
        'A1_COD' : 'Cód Cliente',
        'A1_NOME' : 'Cliente',
        'A3_NREDUZ' : 'Vendedor',
        'C6_VALOR' : 'Valor',
        'C6_QTDVEN' : 'Quantidade',}, 
                inplace=True)

        tabela = tabela[[
        'Pedido',
        'NF',
        'Cód Cliente',
        'Cliente',
        'Quantidade',
        'Valor',
        'Vendedor'
            ]]
    
    df = tabela.sort_values(by='Pedido')            
    
    return df



  
#Função principal
def main():
    
    #Definir tabelas
    sc6010 = Criar_tabela(query_sc6010)
    sa3010 = Criar_tabela(query_sa3010)
    sa1010 = Criar_tabela(query_sa1010)
    sa1010 = sa1010.drop_duplicates(subset=['A1_COD'])
    
    sc6010 = Mesclar_tabelas(sc6010,sa1010,'A1_COD')
    sc6010 = Mesclar_tabelas(sc6010, sa3010,'VENDEDOR')
    
    pedidos_naofaturados = Tabela_pedidos_nao_faturados(sc6010)
    pedidos_naofaturados['C6_VALOR'] = pedidos_naofaturados['C6_VALOR'].apply(formato_moeda)
    
    pedidos_faturados = Tabela_pedidos_faturados(sc6010)
    pedidos_faturados['C6_VALOR'] = pedidos_faturados['C6_VALOR'].apply(formato_moeda)
    
    
    faturamento = Calcular_faturamento(sc6010)
    volume = Calcular_volume(sc6010)
    preco_medio = Calcular_preco_medio(sc6010)
    
    pedidos_faturados = Renomear_colunas(pedidos_faturados)
    pedidos_naofaturados = Renomear_colunas(pedidos_naofaturados)
    
    #Definir pagina STREAMLIT
    
    st.title('Pedidos não Faturados')
    st.dataframe(pedidos_naofaturados, hide_index=True)

    st.title('Pedidos Faturados')
    st.dataframe(pedidos_faturados, hide_index=True)

    col1, col2, col3 = st.columns(3)
    with col1:
            st.metric(label=':red[Faturamento Diário]', value=faturamento)

    with col2:
            st.metric(label='Preço médio', value=preco_medio)
        
    with col3:
            st.metric(label='Volume', value=volume)



main()


