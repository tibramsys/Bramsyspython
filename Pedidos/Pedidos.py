import pandas as pd
import streamlit as st 
import pyodbc
from datetime import datetime
from dotenv import load_dotenv
import os
import time

# load_dotenv()


# host = os.getenv('host')
# user = os.getenv('user')
# password = os.getenv('password')
# database = 'protheus12_producao'

# query_sc6010 = """
# DECLARE @DATE_MIN AS DATE
# SET @DATE_MIN = DATEFROMPARTS(YEAR(GETDATE()),MONTH(GETDATE()),DAY(GETDATE()))

# DECLARE @DATE_MAX AS DATE
# SET @DATE_MAX = DATEFROMPARTS(YEAR(GETDATE()),MONTH(GETDATE()),1)

# SELECT
# 	C6_FILIAL,
# 	C6_ITEM,
# 	C6_PRODUTO,
# 	C6_UM,
# 	C6_QTDVEN,
# 	C6_PRCVEN,
# 	C6_VALOR,
# 	C6_TES,
# 	C6_CF,
# 	C6_CLI,
# 	C6_LOJA,
# 	C6_NOTA,
# 	C6_SERIE,
# 	CONVERT(DATE,C6_DATFAT,112) AS C6_DATFAT,
# 	C6_NUM,
# 	C6_DESCRI,
# 	C6_PRUNIT,
# 	LTRIM(RTRIM(C6_LOTECTL)) AS C6_LOTECTL,
# 	CONVERT(DATE,C6_DTVALID,112) AS C6_DTVALID,
# 	C5_TIPO,
# 	C5_VEND1 AS VENDEDOR,
# 	C5_LIBEROK,
# 	C6_SUGENTR

# FROM
# 	SC6010

# LEFT JOIN SC5010 ON SC6010.C6_FILIAL = SC5010.C5_FILIAL AND SC6010.C6_NUM = SC5010.C5_NUM AND SC6010.C6_CLI = SC5010.C5_CLIENTE



# WHERE 
# 	SC6010.D_E_L_E_T_ = ' '
# 	AND SC6010.C6_CF IN ('5101','5102','5113','5114','5551','6101','6102','6107','6108','6109','6113','6114','6551','6933')
# 	AND SC6010.C6_TES NOT IN ('5AW', '5AY', '5AZ', '5BA', '5BB', '5BC', '6AD', '8LB', '8LC', '8LD', '8LE')
# 	AND CONVERT(DATE,SC6010.C6_SUGENTR,112) >= @DATE_MIN

# """

# query_sa1010 = """
# SELECT 
# 	A1_COD,
# 	A1_NOME,
# 	A1_NREDUZ,
# 	A1_EST

# FROM SA1010
# """

# query_sa3010 = """
# SELECT
# 	A3_COD AS VENDEDOR,
# 	A3_NOME,
# 	A3_NREDUZ,

# 	CASE
# 		WHEN A3_COD = '000004' THEN 'Região A'
# 		WHEN A3_COD = '000044' THEN 'Região B'
# 		WHEN A3_COD = '000038' OR A3_COD = '000027' THEN 'Região C'
# 		WHEN A3_COD = '000025' THEN 'Região Licitação'
# 	END A3_COD_REGIAO

# FROM SA3010
# """

# #Consulta banco de Dados - SC6010
# conn_1 = pyodbc.connect(f'Driver={{ODBC Driver 17 for SQL Server}};Server={host};Database={database};UID={user};PWD={password}')
# cursor_1 = conn_1.cursor()
# cursor_1.execute(query_sc6010)
# rows = cursor_1.fetchall()
# cursor_1.close()

# colunas = [
# 	'C6_FILIAL',
# 	'C6_ITEM',
# 	'C6_PRODUTO',
# 	'C6_UM',
# 	'C6_QTDVEN',
# 	'C6_PRCVEN',
# 	'C6_VALOR',
# 	'C6_TES',
# 	'C6_CF',
# 	'COD',              #ANTIGO C6_CLI
# 	'C6_LOJA',
# 	'C6_NOTA',
# 	'C6_SERIE',
# 	'C6_DATFAT',
# 	'C6_NUM',
# 	'C6_DESCRI',
# 	'C6_PRUNIT',
# 	'C6_LOTECTL',
# 	'C6_DTVALID',
# 	'C5_TIPO',
# 	'VENDEDOR',			#ANTIGO C5_VEND1
# 	'C5_LIBEROK',
# 	'C6_SUGENTR'
# ]
# sc6010 = pd.DataFrame(columns=colunas)

# #Carrega dados da consulta - SC6010
# for row in rows:
# 	dicionario = {
# 		'C6_FILIAL' : row[0],
# 		'C6_ITEM' : row[1],
# 		'C6_PRODUTO' : row[2],
# 		'C6_UM' : row[3],
# 		'C6_QTDVEN' : row[4],
# 		'C6_PRCVEN' : row[5],
# 		'C6_VALOR' : row[6],
# 		'C6_TES' : row[7],
# 		'C6_CF' : row[8],
# 		'COD' : row[9],
# 		'C6_LOJA' : row[10],
# 		'C6_NOTA' : row[11],
# 		'C6_SERIE' : row[12],
# 		'C6_DATFAT' : datetime.strptime(str(row[13]),'%Y-%m-%d'),
# 		'C6_NUM' : row[14],
# 		'C6_DESCRI' : row[15],
# 		'C6_PRUNIT' : row[16],
# 		'C6_LOTECTL' : row[17],
# 		'C6_DTVALID' : row[18],
# 		'C5_TIPO' : row[19],
# 		'VENDEDOR' : row[20],
# 		'C5_LIBEROK' : row[21],
# 		'C6_SUGENTR' : row[22]
# 		}

# 	sc6010.loc[len(sc6010)] = dicionario
	
# #Consulta banco de Dados - SA1010
# conn_2 = pyodbc.connect(f'Driver={{ODBC Driver 17 for SQL Server}};Server={host};Database={database};UID={user};PWD={password}')
# cursor_2 = conn_2.cursor()
# cursor_2.execute(query_sa1010)
# rows = cursor_2.fetchall()
# cursor_2.close()

# colunas2 = [
# 	'COD',              #ANTIGO A1_COD
# 	'A1_NOME',
# 	'A1_NREDUZ',
# 	'A1_EST'
# ]
# sa1010 = pd.DataFrame(columns=colunas2)

# #Carrega dados da consulta - SA1010
# for row in rows:
# 	dicionario = {
# 		'COD' : row[0],
# 		'A1_NOME' : row[1],
# 		'A1_NREDUZ' : row[2],
# 		'A1_EST' : row[3]
# 		}

# 	sa1010.loc[len(sa1010)] = dicionario    
# sa1010 = sa1010.drop_duplicates(subset=['COD'])    
	
# sc6010 = pd.merge(sc6010, sa1010, how='left', on='COD')


# #Consulta banco de Dados - SA3010
# conn_3 = pyodbc.connect(f'Driver={{ODBC Driver 17 for SQL Server}};Server={host};Database={database};UID={user};PWD={password}')
# cursor_3 = conn_3.cursor()
# cursor_3.execute(query_sa3010)
# rows3 = cursor_3.fetchall()
# cursor_3.close()

# colunas_sa3010 = [
# 	'VENDEDOR',
# 	'A3_NOME',
# 	'A3_NREDUZ',
# 	'REGIAO'
# ]

# sa3010 = pd.DataFrame(columns=colunas_sa3010)

# #Carrega dados da consulta - SA3010
# for row in rows3:
# 	dicionario = {
# 		'VENDEDOR' : row[0],
# 		'A3_NOME' : row[1],
# 		'A3_NREDUZ' : row[2],
# 		'REGIAO' : row[3]
# 		}

# 	sa3010.loc[len(sa3010)] = dicionario    

# sc6010_real = pd.merge(sc6010, sa3010, how='left', on='VENDEDOR')



# sc6010 = sc6010_real[['C6_NUM','C6_NOTA','C6_QTDVEN','COD','A1_NOME','C6_VALOR','A3_NREDUZ']]

# sc6010_1 = sc6010.groupby('C6_NOTA')[['C6_VALOR', 'C6_QTDVEN']].sum().reset_index()
# sc6010_1['C6_NOTA'] = sc6010_1['C6_NOTA'].str.strip()

# sc6010_2 = sc6010.loc[sc6010['C6_NOTA'] != ' ']
# sc6010_2 = sc6010_2.drop_duplicates(subset=['C6_NOTA']).reset_index()
# sc6010_2 = sc6010_2.drop(['index','C6_VALOR','C6_QTDVEN'], axis=1)



# for x in sc6010_2:
# 	sc6010_2[x] = sc6010_2[x].str.strip()

# sc6010 = pd.merge(sc6010_2,sc6010_1, how='left', on='C6_NOTA')


# #----- TABELA DE PEDIDOS NÃO FATURADOS -----
# sc6010_nfat = sc6010_real[['C6_NUM','C6_QTDVEN','COD','A1_NOME','C6_VALOR','A3_NREDUZ','C5_LIBEROK']]
# sc6010_nfat = sc6010_nfat.loc[sc6010_nfat['C5_LIBEROK'] == ' ']

# sc6010_nfat_1 = sc6010_nfat.groupby('C6_NUM')[['C6_QTDVEN','C6_VALOR']].sum().reset_index()
# sc6010_nfat_1['C6_NUM'] = sc6010_nfat_1['C6_NUM'].str.strip()

# sc6010_nfat_2 = sc6010_nfat.drop_duplicates(subset=['C6_NUM']).reset_index()
# sc6010_nfat_2 = sc6010_nfat_2.drop(['index','C6_QTDVEN','C6_VALOR','C5_LIBEROK'], axis=1)

# for x in sc6010_nfat_2:
# 	sc6010_nfat_2[x] = sc6010_nfat_2[x].str.strip()

# sc6010_nfat = pd.merge(sc6010_nfat_2, sc6010_nfat_1, how='left', on='C6_NUM')


# def formatar(valor):
# 	return "R${:.2f}".format(valor)

# soma_total = sc6010['C6_VALOR'].sum()
# total = f'R${soma_total:_.2f}'
# total = total.replace('.',',').replace('_','.')

# soma_vol = sc6010['C6_QTDVEN'].sum()
# volume = f'{int(soma_vol)}'

# try:
# 	preco_medio = soma_total / soma_vol
# except:
# 	preco_medio = 0

# if preco_medio != 0:
# 	preco_medio_valor = f'R${preco_medio:,.2f}'
# else:
# 	preco_medio_valor = 'R$0,00'





# sc6010['C6_VALOR'] = sc6010['C6_VALOR'].apply(formatar)
# sc6010_nfat['C6_VALOR'] = sc6010_nfat['C6_VALOR'].apply(formatar)

# sc6010.rename(columns={
# 	'C6_NUM' : 'Pedido',
# 	'C6_NOTA' : 'NF',
# 	'COD' : 'Cód Cliente',
# 	'A1_NOME' : 'Cliente',
# 	'A3_NREDUZ' : 'Vendedor',
# 	'C6_VALOR' : 'Valor',
# 'C6_QTDVEN' : 'Quantidade',}, 
#        inplace=True)

# sc6010 = sc6010[[
# 	'Pedido',
# 	'NF',
# 	'Cód Cliente',
# 	'Cliente',
# 	'Quantidade',
# 	'Valor',
# 	'Vendedor'
# ]]

# sc6010_nfat.rename(columns={
# 	'C6_NUM' : 'Pedido',
# 	'COD' : 'Cód Cliente',
# 	'A1_NOME' : 'Cliente',
# 	'A3_NREDUZ' : 'Vendedor',
# 	'C6_QTDVEN' : 'Quantidade',
# 	'C6_VALOR' : 'Valor'},
#        inplace=True)

# sc6010 = sc6010.sort_values(by='Pedido')



# # #CRIAR TABELA STREAMLIT

# st.title('Pedidos não Faturados')

# st.dataframe(sc6010_nfat, hide_index=True)

# st.title('Pedidos Faturados')

# st.dataframe(sc6010, hide_index=True)

# col1, col2, col3 = st.columns(3)
# with col1:
# 		st.metric(label='Faturamento Diário', value=total)

# with col2:
# 		st.metric(label='Preço médio', value=preco_medio_valor)
	
# with col3:
# 		st.metric(label='Volume', value=volume)

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
	AND SC6010.C6_TES NOT IN ('5AW', '5AY', '5AZ', '5BA', '5BB', '5BC', '6AD', '8LB', '8LC', '8LD', '8LE')
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
    df = tabela_sc6010[['C6_NUM','C6_NOTA','C6_QTDVEN','A1_COD','A1_NOME','C6_VALOR','A3_NREDUZ']] #Seleciona as colunas
    
    df1 = df.loc[df['C6_NOTA'] != ' '] #Filtra apenas os pedidos que possuem NF
    df1 = df1.drop_duplicates(subset=['C6_NOTA']).reset_index() #Remove os valores duplicados
    df1 = df1.drop(['index','C6_VALOR','C6_QTDVEN'], axis=1) #Remove colunas
    
    #Cria uma nova tabela com valores e quantidades somadas
    df2 = df.groupby('C6_NOTA')[['C6_VALOR', 'C6_QTDVEN']].sum().reset_index() #Soma os valores das colunas C6_QTDVEN e C6_VALOR
    
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
    soma = tabela_sc6010['C6_VALOR'].sum()
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
        preco_medio = f'R${preco_medio:,.2f}'
    except:
        preco_medio = 'R$0,00'
        
    return preco_medio


  
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
    pedidos_faturados = Tabela_pedidos_faturados(sc6010)
    
    faturamento = Calcular_faturamento(sc6010)
    volume = Calcular_volume(sc6010)
    preco_medio = Calcular_preco_medio(sc6010)
    
    #Definir pagina STREAMLIT
    
    st.title('Pedidos não Faturados')
    st.dataframe(pedidos_naofaturados, hide_index=True)

    st.title('Pedidos Faturados')
    st.dataframe(pedidos_faturados, hide_index=True)

    col1, col2, col3 = st.columns(3)
    with col1:
            st.metric(label='Faturamento Diário', value=faturamento)

    with col2:
            st.metric(label='Preço médio', value=preco_medio)
        
    with col3:
            st.metric(label='Volume', value=volume)
    
main()




	
