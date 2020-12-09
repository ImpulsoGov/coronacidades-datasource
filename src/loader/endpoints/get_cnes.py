import pandas as pd
from datetime import datetime
import numpy as np
import time
from selenium import webdriver
from time import sleep
from bs4 import BeautifulSoup
from pathlib import Path
import os

from endpoints.helpers import allow_local
from endpoints import get_places_id


def get_citycode(row):
    x = row["city_name"].split(" ")
    x = x[0]
    return x


def get_cityname(row):
    x = row["city_name"]
    x = x[6:]
    return x


@allow_local
def now(config):
    # TODO: ajeitar codigo com a tabela places_ids
    datamunicipios = get_places_id.now(config)

    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--window-size=1420,1080")
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    driver = webdriver.Chrome(
        chrome_options=chrome_options
    )  # chromedriver é instalado via dockerfile, não precisa passar PATH

    # TODO: Separa função get_leiintbr
    urlleitos = "http://tabnet.datasus.gov.br/cgi/deftohtm.exe?cnes/cnv/leiintbr.def"
    driver.get(urlleitos)
    sleep(15)
    element = driver.find_elements_by_xpath("//option[@value='Município']")
    element[0].click()
    element = driver.find_elements_by_xpath("//option[@value='Especialidade']")
    element[1].click()
    element = driver.find_elements_by_class_name("mostra")
    element[0].click()
    df_Leitos_internacao_tot_ago = pd.DataFrame(
        columns=[
            "city_name",
            "cirurgico_tot_ago",
            "clinico_tot_ago",
            "pediatrico_tot_ago",
            "hospital_dia_tot_ago",
        ]
    )
    sleep(15)
    tableRows = driver.find_elements_by_class_name("tabdados")
    html = tableRows[0].get_attribute("innerHTML")
    soup = BeautifulSoup(html, "html.parser")
    x = soup.select("td")
    y = soup.select("tr")
    i = 0
    k = 10
    for j in range(4, len(y)):
        df_Leitos_internacao_tot_ago.loc[i] = [
            x[k].text,
            x[k + 1].text,
            x[k + 2].text,
            x[k + 4].text,
            x[k + 6].text,
        ]
        k = k + 8
        i = i + 1

    element = driver.find_elements_by_class_name("botao_opcao")
    element[4].click()
    sleep(15)
    element = driver.find_elements_by_xpath("//option[@value='Quantidade_existente']")
    element[0].click()
    element = driver.find_elements_by_xpath("//option[@value='Quantidade_SUS']")
    element[0].click()
    element = driver.find_elements_by_class_name("mostra")
    element[0].click()
    df_Leitos_internacao_SUS_ago = pd.DataFrame(
        columns=[
            "city_name",
            "cirurgico_SUS_ago",
            "clinico_SUS_ago",
            "pediatrico_SUS_ago",
            "hospital_dia_SUS_ago",
        ]
    )
    sleep(15)
    tableRows = driver.find_elements_by_class_name("tabdados")
    html = tableRows[0].get_attribute("innerHTML")
    soup = BeautifulSoup(html, "html.parser")
    x = soup.select("td")
    y = soup.select("tr")
    i = 0
    k = 10
    for j in range(4, len(y)):
        df_Leitos_internacao_SUS_ago.loc[i] = [
            x[k].text,
            x[k + 1].text,
            x[k + 2].text,
            x[k + 4].text,
            x[k + 6].text,
        ]
        k = k + 8
        i = i + 1

    element = driver.find_elements_by_class_name("botao_opcao")
    element[4].click()
    sleep(15)
    element = driver.find_elements_by_xpath("//option[@value='Quantidade_SUS']")
    element[0].click()
    element = driver.find_elements_by_xpath("//option[@value='Quantidade_Não_SUS']")
    element[0].click()
    element = driver.find_elements_by_class_name("mostra")
    element[0].click()
    df_Leitos_internacao_nao_SUS_ago = pd.DataFrame(
        columns=[
            "city_name",
            "cirurgico_nao_SUS_ago",
            "clinico_nao_SUS_ago",
            "pediatrico_nao_SUS_ago",
            "hospital_dia_nao_SUS_ago",
        ]
    )
    sleep(15)
    tableRows = driver.find_elements_by_class_name("tabdados")
    html = tableRows[0].get_attribute("innerHTML")
    soup = BeautifulSoup(html, "html.parser")
    x = soup.select("td")
    y = soup.select("tr")
    i = 0
    k = 10
    for j in range(4, len(y)):
        df_Leitos_internacao_nao_SUS_ago.loc[i] = [
            x[k].text,
            x[k + 1].text,
            x[k + 2].text,
            x[k + 4].text,
            x[k + 6].text,
        ]
        k = k + 8
        i = i + 1
    df_leitos = df_Leitos_internacao_tot_ago.merge(
        df_Leitos_internacao_SUS_ago, how="left", on="city_name"
    )
    df_leitos = df_leitos.merge(
        df_Leitos_internacao_nao_SUS_ago, how="left", on="city_name"
    )
    df_leitos["city_id"] = df_leitos.apply(get_citycode, axis=1)
    df_leitos["city_name"] = df_leitos.apply(get_cityname, axis=1)

    # TODO: Separa função get_leiutibr
    urlleitoscomp = (
        "http://tabnet.datasus.gov.br/cgi/deftohtm.exe?cnes/cnv/leiutibr.def"
    )
    driver.get(urlleitoscomp)
    element = driver.find_elements_by_xpath("//option[@value='Município']")
    element[0].click()
    element = driver.find_elements_by_xpath("//option[@value='Leitos_complementares']")
    element[1].click()
    element = driver.find_elements_by_class_name("mostra")
    element[0].click()
    tableRows = driver.find_elements_by_class_name("tabdados")
    html = tableRows[0].get_attribute("innerHTML")
    soup = BeautifulSoup(html, "html.parser")
    df_Leitos_compl_tot_ago = pd.DataFrame(
        columns=[
            "city_name",
            "UTI_adulto_II_COVID_tot_ago",
            "UTI_pediatrica_II_COVID_tot_ago",
            "UTI_adulto_I_tot_ago",
            "UTI_adulto_II_tot_ago",
            "UTI_adulto_III_tot_ago",
            "UTI_pediatrica_I_tot_ago",
            "UTI_pediatrica_II_tot_ago",
            "UTI_pediatrica_III_tot_ago",
        ]
    )
    x = soup.select("td")
    y = soup.select("tr")
    i = 0
    k = 24
    for j in range(4, len(y)):
        df_Leitos_compl_tot_ago.loc[i] = [
            x[k].text,
            x[k + 1].text,
            x[k + 2].text,
            x[k + 5].text,
            x[k + 6].text,
            x[k + 7].text,
            x[k + 8].text,
            x[k + 9].text,
            x[k + 10].text,
        ]
        k = k + 22
        i = i + 1

    element = driver.find_elements_by_class_name("botao_opcao")
    element[4].click()
    sleep(15)
    element = driver.find_elements_by_xpath("//option[@value='Quantidade_existente']")
    element[0].click()
    element = driver.find_elements_by_xpath("//option[@value='Quantidade_SUS']")
    element[0].click()
    element = driver.find_elements_by_class_name("mostra")
    element[0].click()
    tableRows = driver.find_elements_by_class_name("tabdados")
    html = tableRows[0].get_attribute("innerHTML")
    soup = BeautifulSoup(html, "html.parser")
    df_Leitos_compl_SUS_ago = pd.DataFrame(
        columns=[
            "city_name",
            "UTI_adulto_II_COVID_tot_ago",
            "UTI_pediatrica_II_COVID_tot_ago",
            "UTI_adulto_I_tot_ago",
            "UTI_adulto_II_tot_ago",
            "UTI_adulto_III_tot_ago",
            "UTI_pediatrica_I_tot_ago",
            "UTI_pediatrica_II_tot_ago",
            "UTI_pediatrica_III_tot_ago",
        ]
    )
    x = soup.select("td")
    y = soup.select("tr")
    i = 0
    k = 24
    for j in range(4, len(y)):
        df_Leitos_compl_tot_ago.loc[i] = [
            x[k].text,
            x[k + 1].text,
            x[k + 2].text,
            x[k + 5].text,
            x[k + 6].text,
            x[k + 7].text,
            x[k + 8].text,
            x[k + 9].text,
            x[k + 10].text,
        ]
        k = k + 22
        i = i + 1
    element = driver.find_elements_by_class_name("botao_opcao")
    element[4].click()
    sleep(15)
    element = driver.find_elements_by_xpath("//option[@value='Quantidade_SUS']")
    element[0].click()
    element = driver.find_elements_by_xpath("//option[@value='Quantidade_Não_SUS']")
    element[0].click()
    element = driver.find_elements_by_class_name("mostra")
    element[0].click()
    tableRows = driver.find_elements_by_class_name("tabdados")
    html = tableRows[0].get_attribute("innerHTML")
    soup = BeautifulSoup(html, "html.parser")
    df_Leitos_compl_nao_SUS_ago = pd.DataFrame(
        columns=[
            "city_name",
            "UTI_adulto_II_COVID_tot_ago",
            "UTI_pediatrica_II_COVID_tot_ago",
            "UTI_adulto_I_tot_ago",
            "UTI_adulto_II_tot_ago",
            "UTI_adulto_III_tot_ago",
            "UTI_pediatrica_I_tot_ago",
            "UTI_pediatrica_II_tot_ago",
            "UTI_pediatrica_III_tot_ago",
        ]
    )
    x = soup.select("td")
    y = soup.select("tr")
    i = 0
    k = 24
    for j in range(4, len(y)):
        df_Leitos_compl_tot_ago.loc[i] = [
            x[k].text,
            x[k + 1].text,
            x[k + 2].text,
            x[k + 5].text,
            x[k + 6].text,
            x[k + 7].text,
            x[k + 8].text,
            x[k + 9].text,
            x[k + 10].text,
        ]
        k = k + 22
        i = i + 1
    df_leitos_comp = df_Leitos_compl_tot_ago.merge(
        df_Leitos_compl_SUS_ago, how="left", on="city_name"
    )
    df_leitos_comp = df_leitos_comp.merge(
        df_Leitos_compl_nao_SUS_ago, how="left", on="city_name"
    )
    df_leitos_comp["city_id"] = df_leitos_comp.apply(get_citycode, axis=1)
    df_leitos_comp["city_name"] = df_leitos_comp.apply(get_cityname, axis=1)

    # TODO: Separa função get_equipobr
    urlresp = "http://tabnet.datasus.gov.br/cgi/deftohtm.exe?cnes/cnv/equipobr.def"
    driver.get(urlresp)
    sleep(15)
    element = driver.find_elements_by_xpath("//option[@value='Município']")
    element[0].click()
    element = driver.find_elements_by_xpath("//option[@value='Equipamento']")
    element[1].click()
    element = driver.find_elements_by_class_name("mostra")
    element[0].click()
    tableRows = driver.find_elements_by_class_name("tabdados")
    html = tableRows[0].get_attribute("innerHTML")
    soup = BeautifulSoup(html, "html.parser")
    df_respiradores = pd.DataFrame(columns=["city_name", "Respiradores_ago"])
    x = soup.select("td")
    y = soup.select("tr")
    i = 0
    k = 24
    for j in range(4, len(y)):
        df_respiradores.loc[i] = [x[k].text, x[k + 1].text]
        k = k + 22
        i = i + 1

    df_cnes = df_leitos.merge(df_leitos_comp, how="left", on=["city_id", "city_name"])
    df_cnes = df_cnes.merge(df_respiradores, how="left", on=["city_name"])
    df_cnes["city_id"] = df_cnes["city_id"].astype(str).astype(int)

    # TODO: não usar _y, remover colunas que vao ser substituidas no
    # df_cnes - checar erro em city_id
    df_cnes = datamunicipios.merge(
        df_cnes, how="left", on=["city_id"], suffixes=["", "_y"]
    )

    # TODO: Separar função de tratamento dos dados  - muita repetição
    # aqui, enxugar o código (é necessário mudar o tipo da coluna 3
    # vezes?, passar a usar loop / outra função para não repetir o mesmo tratamento nas colunas)
    df_cnes = df_cnes.replace({"-": 0}, regex=True)
    df_cnes = df_cnes.replace(np.nan, 0, regex=True)
    df_cnes = df_cnes.rename(
        columns={"city_id": "municipio_6", "municipio_6": "city_id"}
    )
    df_new = pd.DataFrame(
        columns=[
            "city_id",
            "city_name",
            "state_id",
            "number_ventilators",
            "number_beds",
            "number_icu_beds",
            "number_covid_icu_beds",
            "last_updated_number_ventilators",
            "last_updated_number_beds",
            "last_updated_number_icu_beds",
            "last_updated_number_covid_icu_beds",
        ]
    )
    df_new["city_id"] = df_cnes["city_id"]
    df_new["city_name"] = df_cnes["city_name"]
    df_new["state_id"] = df_cnes["id_estado"]
    df_new["number_ventilators"] = df_cnes["Respiradores_ago"]
    df_cnes["cirurgico_tot_ago"] = (
        df_cnes["cirurgico_tot_ago"].astype(str).astype(float).astype(int)
    )
    df_cnes["clinico_tot_ago"] = (
        df_cnes["clinico_tot_ago"].astype(str).astype(float).astype(int)
    )
    df_cnes["hospital_dia_tot_ago"] = (
        df_cnes["hospital_dia_tot_ago"].astype(str).astype(float).astype(int)
    )
    df_cnes["UTI_adulto_I_tot_ago"] = (
        df_cnes["UTI_adulto_I_tot_ago"].astype(str).astype(float).astype(int)
    )
    df_cnes["UTI_adulto_II_tot_ago"] = (
        df_cnes["UTI_adulto_II_tot_ago"].astype(str).astype(float).astype(int)
    )
    df_cnes["UTI_adulto_III_tot_ago"] = (
        df_cnes["UTI_adulto_III_tot_ago"].astype(str).astype(float).astype(int)
    )
    df_cnes["UTI_adulto_II_COVID_SUS_ago"] = (
        df_cnes["UTI_adulto_II_COVID_SUS_ago"].astype(str).astype(float).astype(int)
    )
    df_cnes["UTI_adulto_II_COVID_nao_SUS_ago"] = (
        df_cnes["UTI_adulto_II_COVID_nao_SUS_ago"].astype(str).astype(float).astype(int)
    )
    df_cnes["UTI_pediatrica_II_COVID_SUS_ago"] = (
        df_cnes["UTI_pediatrica_II_COVID_SUS_ago"].astype(str).astype(float).astype(int)
    )
    df_cnes["UTI_pediatrica_II_COVID_nao_SUS_ago"] = (
        df_cnes["UTI_pediatrica_II_COVID_nao_SUS_ago"]
        .astype(str)
        .astype(float)
        .astype(int)
    )
    df_new["number_beds"] = (
        df_cnes["cirurgico_tot_ago"]
        + df_cnes["clinico_tot_ago"]
        + df_cnes["hospital_dia_tot_ago"]
    )
    df_new["number_icu_beds"] = (
        df_cnes["UTI_adulto_I_tot_ago"]
        + df_cnes["UTI_adulto_II_tot_ago"]
        + df_cnes["UTI_adulto_III_tot_ago"]
    )
    df_new["number_covid_icu_beds"] = (
        df_cnes["UTI_adulto_II_COVID_SUS_ago"]
        + df_cnes["UTI_adulto_II_COVID_nao_SUS_ago"]
        + df_cnes["UTI_pediatrica_II_COVID_SUS_ago"]
        + df_cnes["UTI_pediatrica_II_COVID_nao_SUS_ago"]
    )
    df_new["last_updated_number_ventilators"] = datetime.now().strftime("%Y-%m-%d")
    df_new["last_updated_number_beds"] = datetime.now().strftime("%Y-%m-%d")
    df_new["last_updated_number_icu_beds"] = datetime.now().strftime("%Y-%m-%d")
    df_new["last_updated_number_covid_icu_beds"] = datetime.now().strftime("%Y-%m-%d")
    return df_new


TESTS = {}
