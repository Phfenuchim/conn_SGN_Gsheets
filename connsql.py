import pyodbc
import pandas as pd
import gspread
import datetime
from gspread_dataframe import  set_with_dataframe

def conectsql(bd):
    # Define the server, database, username, and password
    SERVER = '787307cf6469.sn.mynetname.net,6335'
    USERNAME = 'lpbacesso'
    PASSWORD = '1234567890'
    DATABASE = f'{bd}'
    # Create a connection string
    connection_string = (
        f'DRIVER={{SQL Server}};'
        f'SERVER={SERVER};'
        f'DATABASE={DATABASE};'
        f'UID={USERNAME};'
        f'PWD={PASSWORD};'
        f'Trusted_Connection=no;'
    )
    conn = pyodbc.connect(connection_string)
    print('conexão BD')
    return conn

def consul(conn): 
    try:
        # Define a consulta SQL
        sql_query = "SELECT * FROM CLIENTES"
        # Cria um novo cursor
        cursor = conn.cursor()
        # Executa a consulta SQL
        cursor.execute(sql_query)
        # Busca todas as linhas
        rows = cursor.fetchall()
        # Obtém os nomes das colunas da descrição do cursor
        columns = [column[0] for column in cursor.description]
        # Cria um DataFrame a partir das linhas e nomes das colunas
        df = pd.DataFrame.from_records(rows, columns=columns)
        # Fecha a conexão
        conn.close()
        return df
    except Exception as e:
        print(f"Erro ao consultar dados: {e}")
        return None

def login():
    gc = gspread.service_account(filename='creads.json')
    sheet = gc.open_by_key('10tm5JnXLvNvtjNFjXmQMtDObeDVbzYCSuO-exQtCSMs')
    aba = sheet.worksheet('testeconn')
    print("Login google sheets")
    return aba

def escreverDfGoogleSheets(df, aba):
    intervalo = "A2:Z"
    aba.batch_clear([intervalo])
    set_with_dataframe(aba,df,row=2)
    print("Dados gravados com sucesso no Google Sheets!")

def agoraAt(aba):
    agora = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    aba.update_cell(1,4, f"{agora}")

bd = 'LPB'
conn = conectsql(bd)
sqlDF = consul(conn)
aba = login()
escreverDfGoogleSheets(sqlDF, aba)
agoraAt(aba)




