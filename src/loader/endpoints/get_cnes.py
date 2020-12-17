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
from utils import download_from_drive
from logger import logger

def get_date(updatedate):
    ano = updatedate[4:]
    meses = {'Jan':1, 'Fev':2, 'Mar':3, 'Abr':4, 'Mai':5, 'Jun':6, 'Jul':7, 'Ago':8, 'Set':9, 'Out':10, 'Nov':11, 'Dez':12}
    mes = str(meses[updatedate[:3]])
    x = ano+'-'+mes+'-'+'01'
    return x

def get_citycode(row):
    x = row["city_name"].split(" ")
    x = x[0]
    return x


def get_cityname(row):
    x = row["city_name"]
    x = x[6:-1]
    return x.strip("\n")


def get_leitos(driver, url):
    driver.get(url)
    sleep(15)
    element = driver.find_elements_by_xpath("//option[@value='Município']")
    element[0].click()
    element = driver.find_elements_by_xpath("//option[@value='Especialidade']")
    element[1].click()
    updatedate = driver.find_element_by_id("A")
    updatedate = updatedate.text[0:8]
    element = driver.find_elements_by_class_name("mostra")
    element[0].click()
    df_leitos = pd.DataFrame(
        columns=[
            "city_name",
            "cirurgico_tot_ago",
            "clinico_tot_ago",
            "pediatrico_tot_ago",
            "hospital_dia_tot_ago",
        ]
    )
    tableRows = driver.find_elements_by_class_name("tabdados")
    html = tableRows[0].get_attribute("innerHTML")
    soup = BeautifulSoup(html, "html.parser")
    x = soup.select("td")
    y = soup.select("tr")
    i = 0
    k = 10
    for j in range(4, len(y)):
        df_leitos.loc[i] = [
            x[k].text,
            x[k + 1].text,
            x[k + 2].text,
            x[k + 4].text,
            x[k + 7].text,
        ]
        k = k + 8
        i = i + 1
    df_leitos["city_id"] = df_leitos.apply(get_citycode, axis=1)
    df_leitos["city_name"] = df_leitos.apply(get_cityname, axis=1)
    return df_leitos, updatedate


def get_respiradores(driver, url):
    driver.get(url)
    sleep(15)
    element = driver.find_elements_by_xpath("//option[@value='Município']")
    element[0].click()
    element = driver.find_elements_by_xpath("//option[@value='Equipamento']")
    element[1].click()
    element = driver.find_elements_by_class_name("mostra")
    sleep(15)
    element[0].click()
    sleep(15)
    tableRows = driver.find_elements_by_class_name("tabdados")
    html = tableRows[0].get_attribute("innerHTML")
    soup = BeautifulSoup(html, "html.parser")
    df_respiradores = pd.DataFrame(columns=["city_name", "Respiradores_ago"])
    x = soup.select("td")
    y = soup.select("tr")
    i = 0
    k = 85
    for j in range(4, len(y)):
        df_respiradores.loc[i] = [x[k].text, x[k + 54].text]
        k = k + 83
        i = i + 1
    df_respiradores["city_name"] = df_respiradores.apply(get_cityname, axis=1)
    return df_respiradores


def get_urlleitoscomp(driver, url):
    driver.get(url)
    element = driver.find_elements_by_xpath("//option[@value='Município']")
    element[0].click()
    element = driver.find_elements_by_xpath("//option[@value='Leitos_complementares']")
    element[1].click()
    element = driver.find_elements_by_class_name("mostra")
    element[0].click()
    tableRows = driver.find_elements_by_class_name("tabdados")
    html = tableRows[0].get_attribute("innerHTML")
    soup = BeautifulSoup(html, "html.parser")
    df_leitos_comp = pd.DataFrame(
        columns=[
            "city_name",
            "UTI_adulto_I_tot_ago",
            "UTI_adulto_II_tot_ago",
            "UTI_adulto_III_tot_ago",
        ]
    )
    x = soup.select("td")
    y = soup.select("tr")
    i = 0
    k = 24
    for j in range(4, len(y)):
        df_leitos_comp.loc[i] = [x[k].text, x[k + 5].text, x[k + 6].text, x[k + 7].text]
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
            "UTI_adulto_II_COVID_SUS_ago",
            "UTI_pediatrica_II_COVID_SUS_ago",
        ]
    )
    x = soup.select("td")
    y = soup.select("tr")
    i = 0
    k = 24
    for j in range(4, len(y)):
        df_Leitos_compl_SUS_ago.loc[i] = [x[k].text, x[k + 1].text, x[k + 2].text]
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
            "UTI_adulto_II_COVID_nao_SUS_ago",
            "UTI_pediatrica_II_COVID_nao_SUS_ago",
        ]
    )
    x = soup.select("td")
    y = soup.select("tr")
    i = 0
    k = 24
    for j in range(4, len(y)):
        df_Leitos_compl_nao_SUS_ago.loc[i] = [x[k].text, x[k + 1].text, x[k + 2].text]
        k = k + 22
        i = i + 1
    df_leitos_comp = df_leitos_comp.merge(
        df_Leitos_compl_SUS_ago, how="left", on="city_name"
    )
    df_leitos_comp = df_leitos_comp.merge(
        df_Leitos_compl_nao_SUS_ago, how="left", on="city_name"
    )
    df_leitos_comp["city_id"] = df_leitos_comp.apply(get_citycode, axis=1)
    df_leitos_comp["city_name"] = df_leitos_comp.apply(get_cityname, axis=1)
    return df_leitos_comp


@allow_local
def now(config):
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--window-size=1420,1080")
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")

    driver = webdriver.Chrome(
        chrome_options=chrome_options
    )  # chromedriver é instalado via dockerfile

    # Pega dados de Leitos pela especialidade de todos os municipios #
    logger.info("Baixando dados de leitos")
    urlleitos = "http://tabnet.datasus.gov.br/cgi/deftohtm.exe?cnes/cnv/leiintbr.def"
    df_leitos, updatedate = get_leitos(driver, urlleitos)
    updatedate = get_date(updatedate)
    print(df_leitos.nunique())

    # Pega dados de Leitos complementares de todos os municipios #
    logger.info("Baixando dados de leitos UTI")
    urlleitoscomp = (
        "http://tabnet.datasus.gov.br/cgi/deftohtm.exe?cnes/cnv/leiutibr.def"
    )
    df_leitos_comp = get_urlleitoscomp(driver, urlleitoscomp)
    print(df_leitos_comp.nunique())

    # Pega dados de Respiradores dos Municipios #
    logger.info("Baixando dados de respiradores")
    urlresp = "http://tabnet.datasus.gov.br/cgi/deftohtm.exe?cnes/cnv/equipobr.def"
    df_respiradores = get_respiradores(driver, urlresp)
    print(df_respiradores.nunique())

    # Une os diferentes dataframes #
    df_cnes = df_leitos.merge(df_leitos_comp, how="left", on=["city_id", "city_name"])
    # df_cnes = df_cnes.merge(df_respiradores, how="left", on=["city_name"])
    logger.info("Une dados de leitos, leitos UTI e respiradores")
    print(df_cnes.nunique())

    df_cnes["city_id"] = df_cnes["city_id"].astype(int)

    df_cnes = df_cnes.replace({"-": 0}, regex=True)
    df_cnes = df_cnes.replace(np.nan, 0, regex=True)

    columns = [
        "cirurgico_tot_ago",
        "hospital_dia_tot_ago",
        "hospital_dia_tot_ago",
        "UTI_adulto_I_tot_ago",
        "UTI_adulto_II_tot_ago",
        "UTI_adulto_III_tot_ago",
        "UTI_adulto_II_COVID_SUS_ago",
        "UTI_adulto_II_COVID_nao_SUS_ago",
        "UTI_pediatrica_II_COVID_SUS_ago",
        "UTI_adulto_II_COVID_nao_SUS_ago",
        # "Respiradores_ago",
    ]

    for col in columns:
        df_cnes[col] = df_cnes[col].astype(str).astype(float).astype(int)

    # df_cnes = df_cnes.rename(columns={"Respiradores_ago": "number_ventilators"})

    df_cnes["number_beds"] = (
        df_cnes["cirurgico_tot_ago"]
        + df_cnes["clinico_tot_ago"]
        + df_cnes["hospital_dia_tot_ago"]
    )
    df_cnes["number_icu_beds"] = (
        df_cnes["UTI_adulto_I_tot_ago"]
        + df_cnes["UTI_adulto_II_tot_ago"]
        + df_cnes["UTI_adulto_III_tot_ago"]
    )
    df_cnes["number_covid_icu_beds"] = (
        df_cnes["UTI_adulto_II_COVID_SUS_ago"]
        + df_cnes["UTI_adulto_II_COVID_nao_SUS_ago"]
        + df_cnes["UTI_pediatrica_II_COVID_SUS_ago"]
        + df_cnes["UTI_pediatrica_II_COVID_nao_SUS_ago"]
    )

    # Da merge com os dados de populacao
    places_ids = get_places_id.now(config)

    # Cria coluna de IBGE 6 dígitos para match
    places_ids["city_id_7d"] = places_ids["city_id"]
    places_ids["city_id"] = places_ids["city_id"].str.apply(lambda x: x[:-1])

    df_cnes = places_ids.merge(df_cnes, how="left", on=["city_id"], suffixes=["", "_y"])

    df_cnes["city_id"] = df_cnes["city_id_7d"]
    df_cnes = df_cnes.drop(columns="city_id_7d")

    df_pop = download_from_drive(config["br"]["drive_paths"]["cities_population"])[
        [
            "country_iso",
            "country_name",
            "state_id",
            "state_name",
            "city_id",
            "city_name",
            "population",
        ]
    ]

    df_cnes = pd.merge(df_cnes, df_pop, on="city_id", how="left", suffixes=("", "_y"))
    df_cnes = df_cnes.drop(
        [
            "state_name_y",
            "UTI_pediatrica_II_COVID_nao_SUS_ago",
            "city_name_y",
            "pediatrico_tot_ago",
            "UTI_adulto_II_COVID_SUS_ago",
            "UTI_pediatrica_II_COVID_SUS_ago",
            "UTI_adulto_II_COVID_nao_SUS_ago",
            "state_id_y",
            "cirurgico_tot_ago",
            "clinico_tot_ago",
            "hospital_dia_tot_ago",
            "UTI_adulto_I_tot_ago",
            "UTI_adulto_II_tot_ago",
            "UTI_adulto_III_tot_ago",
        ],
        axis=1,
    )
    todayday = datetime.now().strftime("%Y-%m-%d")
    (
        df_cnes["last_updated_number_ventilators"],
        df_cnes["last_updated_number_beds"],
        df_cnes["last_updated_number_icu_beds"],
        df_cnes["last_updated_number_covid_icu_beds"],
        df_cnes["author_number_beds"],
        df_cnes["author_number_ventilators"],
        df_cnes["author_number_icu_beds"],
    ) = (
        todayday,
        todayday,
        todayday,
        todayday,
        "DataSUS",
        "DataSUS",
        "DataSUS",
    )
    return df_cnes


TESTS = {
    "data is not pd.DataFrame": lambda df: isinstance(df, pd.DataFrame),
    "more than 5570 cities": lambda df: len(df["city_id"].unique()) <= 5570,
    "no negative beds or icu beds": lambda df: len(
        df.query("number_beds < 0 | number_icu_beds < 0")
    )
    == 0,
    "all zero beds": lambda df: len(df.query("number_beds == 0")) != len(df),
    "all zero icu beds": lambda df: len(df.query("number_icu_beds == 0")) != len(df),
}
