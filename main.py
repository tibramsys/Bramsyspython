import streamlit as st
import pyodbc
import os
from dotenv import load_dotenv
import time

st.set_page_config(layout='wide')
load_dotenv()

# Função para autenticar o usuário
def autenticar_usuario(usuario, senha):
    
    #Credenciais do banco de dados
    host = os.getenv('DB_HOST')
    user = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')
    database = os.getenv('DB_NAME')
    
    #Criar conexão com banco de dados e tabela de usuarios
    conn = pyodbc.connect(f'Driver={{ODBC Driver 17 for SQL Server}};Server={host};Database={database};UID={user};PWD={password}')
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM usuarios WHERE login = ? AND senha = ?", (usuario, senha))
    user = cursor.fetchone()
    conn.close()
    if user:
        return True
    return False

def login():
    """Cria a pagina de login
    """
    st.title("Login")
    username = st.text_input("Usuário").lower().strip()
    password = st.text_input("Senha", type="password")
        
    if st.button("Entrar"):
        if username == '' or password == '':
            st.warning('Inserir usuario e senha')
        else:
            login_usuario = autenticar_usuario(username, password)
            
            if login_usuario == True:
                st.success("Login realizado com sucesso!")
                time.sleep(2)
                st.session_state.logged_in = True
                st.rerun()
            else:
                st.error('Usuário/senha inválidos')

def logout():
    st.markdown('Aviso')
    st.write('Para atualizar a pagina atual selecione a mesma pagina na barra lateral')
    if st.button("Log out"):
        st.session_state.logged_in = False
        st.rerun()


if "logged_in" not in st.session_state:
    st.session_state.logged_in = False

st.image("static/logo-site.png")

login_page = st.Page(login, title="Log in", icon=":material/login:")
logout_page = st.Page(logout, title="Log out", icon=":material/logout:")

dashboard = st.Page("Vendas/dashboard.py", title="Dashboard", icon=":material/dashboard:", default=True)
bugs = st.Page("Vendas/bugs.py", title="Bug Vendas", icon=":material/bug_report:")
alerts = st.Page("Vendas/alerts.py", title="System alerts", icon=":material/notification_important:")
search = st.Page("Pedidos/search.py", title="Search")
history = st.Page("Pedidos/history.py", title="History")
pedidos = st.Page("Pedidos/Pedidos.py", title=" Faturados", icon=":material/check_small:")

if st.session_state.logged_in:

    pg = st.navigation(
        {
            "Conta": [logout_page],
            "Pedidos": [pedidos],
            # "Vendas": [dashboard, bugs, alerts],
            
        }
    )
else:
    pg = st.navigation([login_page])

pg.run()
