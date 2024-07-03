import pandas as pd
import gspread
import datetime
from bs4 import BeautifulSoup
from gspread_dataframe import  set_with_dataframe
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import time

def pageHTML():
    # Create Chrome options
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")  # Use the headless mode

    # Initialize the Chrome driver
    driver = webdriver.Chrome(options=chrome_options)
    #driver = webdriver.Chrome()

    driver.get("https://www.visaovip.com/lista-preco")

    labelElement = driver.find_element(By.ID, "dtProdutosListaPreco:j_id14")
    labelElement.send_keys("Todos") 

    time.sleep(15)

    with open("Lista de Precos.html", "w", encoding="utf-8") as arquivo:
        arquivo.write(driver.page_source)
        print("HTML baixado")
    driver.quit()

def load_html_content(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        html_content = file.read()
        print("Dados carragados")
    return html_content

def parse_html_to_dataframe(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    idtable = "dtProdutosListaPreco_data"
    table = soup.find(id=idtable)

    codigo_list, nome_list, categoria_list, marca_list, valor_list = [], [], [], [], []

    for row in table.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) >= 5:
            codigo = cells[1].find("span", class_="inputTextPequenoPadraoVedoble").text.strip()
            nome = cells[2].text.strip().replace("Nome", "")
            categoria = cells[3].text.strip().replace("Categoria","")
            marca = cells[4].text.strip().replace("Marca","")
            valor = cells[5].text.strip().replace("Valor", "")

            codigo_list.append(codigo)
            nome_list.append(nome)
            categoria_list.append(categoria)
            marca_list.append(marca)
            valor_list.append(valor)

    df = pd.DataFrame({
        "Codigo": codigo_list,
        "Nome": nome_list,
        "Categoria": categoria_list,
        "Marca": marca_list,
        "Valor": valor_list
    })

    df[["Moeda", "Valor"]] = df["Valor"].str.split(expand=True)
    print("Dataframe criado")
    return df

def escreverDfGoogleSheets(df, aba):
    intervalo = "A3:F"
    aba.batch_clear([intervalo])

    set_with_dataframe(aba,df,row=2)
    print("Dados gravados com sucesso no Google Sheets!")

def login():
    gc = gspread.service_account(filename='creads.json')
    sheet = gc.open_by_key('1VMWUD3EsQcC75n4Lub3iRH7OPO2IS90gs6SC-oytzEA')
    aba = sheet.worksheet('Base - Visão VIP')
    return aba

def agoraAt(aba):
    agora = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    # Formato da data e hora
    aba.update_cell(1,4, f"{agora}")


pageHTML()
html_content = load_html_content('Lista de PreCos.html')
productDf = parse_html_to_dataframe(html_content)
aba = login()
escreverDfGoogleSheets(productDf, aba)
agoraAt(aba)
