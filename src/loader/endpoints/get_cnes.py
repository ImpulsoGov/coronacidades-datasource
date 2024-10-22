import pandas as pd
import numpy as np
import re
from io import StringIO
from selenium import webdriver
from time import sleep
from bs4 import BeautifulSoup

from endpoints.helpers import allow_local
from endpoints import get_places_id
from utils import download_from_drive
from logger import logger


def get_date(updatedate):
    ano = updatedate[4:]
    meses = {
        "Jan": 1,
        "Fev": 2,
        "Mar": 3,
        "Abr": 4,
        "Mai": 5,
        "Jun": 6,
        "Jul": 7,
        "Ago": 8,
        "Set": 9,
        "Out": 10,
        "Nov": 11,
        "Dez": 12,
    }
    mes = str(meses[updatedate[:3]])
    x = ano + "-" + mes + "-" + "01"
    return x


def get_city_name(raw_name):
    """Get city name from a string with city id and name separated by a space.
    """
    match_obj = re.search(r"(?<=[0-9]{6} )(?P<city_name>.*)$", raw_name)
    if match_obj:
        return match_obj.group("city_name")


def get_city_id(raw_name):
    """Get city id from a string with city id and name separated by a space.
    """
    match_obj = re.search(r"^(?P<city_id>[0-9]{6})(?= )", raw_name)
    if match_obj:
        return match_obj.group("city_id")


def treat_city_name(df):
    df["city_id"] = df["city_name"].apply(get_city_id)
    df["city_name"] = df["city_name"].apply(get_city_name)
    return df


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
            "cirurgico_tot",
            "clinico_tot",
            "pediatrico_tot",
            "hospital_dia_tot",
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

    df_leitos = treat_city_name(df_leitos)

    return df_leitos, updatedate


def get_respiradores(driver, url):
    driver.get(url)
    sleep(15)
    element = driver.find_elements_by_xpath("//option[@value='Município']")
    element[0].click()
    element = driver.find_elements_by_xpath("//option[@value='Equipamento']")
    element[1].click()
    element = driver.find_elements_by_xpath("//input[@value='prn']")
    element[0].click()
    element = driver.find_elements_by_class_name("mostra")
    element[0].click()
    table = StringIO(driver.find_elements_by_tag_name("pre")[0].text)
    df_respiradores = pd.read_csv(
        table,
        sep=";",
        usecols=["Município", ".. RESPIRADOR/VENTILADOR"],
        na_values=["-"],
    ).rename(columns={
        "Município": "city_name",
        ".. RESPIRADOR/VENTILADOR": "number_ventilators",
    })
    table.close()

    df_respiradores = treat_city_name(df_respiradores)
    df_respiradores = df_respiradores.dropna(
        subset=["city_id", "city_name"]
    ).fillna(0).convert_dtypes()

    return df_respiradores


def get_urlleitoscomp(driver, url):

    # Total de leitos de UTI Adulto
    driver.get(url)
    sleep(10)
    element = driver.find_elements_by_xpath("//option[@value='Município']")
    element[0].click()
    element = driver.find_elements_by_xpath("//option[@value='Leitos_complementares']")
    element[1].click()
    element = driver.find_elements_by_class_name("mostra")
    element[0].click()
    sleep(8)
    df_leitos_comp = (
        pd.read_html(
            driver.page_source,
            attrs={"class": "tabdados"},
            skiprows=[0, 2],
            header=0,
            na_values=["-"],
        )[0].iloc[:, [0, 5, 6, 7]]
        .rename(
            columns={
                "Município": "city_name",
                "UTI adulto I": "UTI_adulto_I_tot",
                "UTI adulto II": "UTI_adulto_II_tot",
                "UTI adulto III": "UTI_adulto_III_tot",
            }
        )
    )
    df_leitos_comp = treat_city_name(df_leitos_comp)
    df_leitos_comp = df_leitos_comp.dropna(
        subset=["city_id", "city_name"]
    ).fillna(0).convert_dtypes()

    # Leitos COVID-19 SUS
    element = driver.find_elements_by_class_name("botao_opcao")
    element[4].click()
    sleep(15)
    element = driver.find_elements_by_xpath("//option[@value='Quantidade_existente']")
    element[0].click()
    element = driver.find_elements_by_xpath("//option[@value='Quantidade_SUS']")
    element[0].click()
    element = driver.find_elements_by_class_name("mostra")
    element[0].click()
    sleep(8)
    df_Leitos_compl_SUS = (
        pd.read_html(
            driver.page_source,
            attrs={"class": "tabdados"},
            skiprows=[0, 2],
            header=0,
            na_values=["-"],
        )[0].iloc[:, [0, 1, 2]]
        .rename(columns={
            "Município": "city_name",
            "UTI adulto II COVID-19": "UTI_adulto_II_COVID_SUS",
            "UTI pediátrica II COVID-19": "UTI_pediatrica_II_COVID_SUS",
        })
    )
    df_Leitos_compl_SUS = treat_city_name(df_Leitos_compl_SUS)
    df_Leitos_compl_SUS = df_Leitos_compl_SUS.dropna(
        subset=["city_id", "city_name"]
    ).fillna(0).convert_dtypes()

    # Leitos COVID-19 particulares
    element = driver.find_elements_by_class_name("botao_opcao")
    element[4].click()
    sleep(15)
    element = driver.find_elements_by_xpath("//option[@value='Quantidade_SUS']")
    element[0].click()
    element = driver.find_elements_by_xpath("//option[@value='Quantidade_Não_SUS']")
    element[0].click()
    element = driver.find_elements_by_class_name("mostra")
    element[0].click()
    df_Leitos_compl_nao_SUS = (
        pd.read_html(
            driver.page_source,
            attrs={"class": "tabdados"},
            skiprows=[0, 2],
            header=0,
            na_values=["-"],
        )[0].iloc[:, [0, 1, 2]]
        .rename(columns={
            "Município": "city_name",
            "UTI adulto II COVID-19": "UTI_adulto_II_COVID_nao_SUS",
            "UTI pediátrica II COVID-19": "UTI_pediatrica_II_COVID_nao_SUS",
        })
    )
    df_Leitos_compl_nao_SUS = treat_city_name(df_Leitos_compl_nao_SUS)
    df_Leitos_compl_nao_SUS = df_Leitos_compl_nao_SUS.dropna(
        subset=["city_id", "city_name"]
    ).fillna(0).convert_dtypes()

    # Juntar datasets
    df_leitos_comp = df_leitos_comp.merge(
        df_Leitos_compl_SUS, how="left", on=["city_id", "city_name"]
    )
    df_leitos_comp = df_leitos_comp.merge(
        df_Leitos_compl_nao_SUS, how="left", on=["city_id", "city_name"]
    )

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
    # Ultima data de atualizacao do dado CNES
    updatedate = get_date(updatedate)

    # Pega dados de Leitos complementares de todos os municipios #
    logger.info("Baixando dados de leitos UTI")
    urlleitoscomp = (
        "http://tabnet.datasus.gov.br/cgi/deftohtm.exe?cnes/cnv/leiutibr.def"
    )
    df_leitos_comp = get_urlleitoscomp(driver, urlleitoscomp)

    # Pega dados de Respiradores dos Municipios #
    logger.info("Baixando dados de respiradores")
    urlresp = "http://tabnet.datasus.gov.br/cgi/deftohtm.exe?cnes/cnv/equipobr.def"
    df_respiradores = get_respiradores(driver, urlresp)

    # Une os diferentes dataframes #
    df_cnes = df_leitos.merge(df_leitos_comp, how="left", on=["city_id", "city_name"])
    df_cnes = df_cnes.merge(df_respiradores, how="left", on=["city_id", "city_name"])
    logger.info("Une dados de leitos, leitos UTI e respiradores")

    # df_cnes["city_id"] = df_cnes["city_id"].astype(str)

    df_cnes = df_cnes.replace({"-": 0}, regex=True)
    df_cnes = df_cnes.replace(np.nan, 0, regex=True)

    # Conserta tipos de colunas
    resources = [
        "cirurgico_tot",
        "clinico_tot",
        "hospital_dia_tot",
        "UTI_adulto_I_tot",
        "UTI_adulto_II_tot",
        "UTI_adulto_III_tot",
        "UTI_adulto_II_COVID_SUS",
        "UTI_adulto_II_COVID_nao_SUS",
        "UTI_pediatrica_II_COVID_SUS",
        "UTI_pediatrica_II_COVID_nao_SUS",
        "number_ventilators",
    ]

    for col in resources:
        df_cnes[col] = df_cnes[col].astype(str).astype(float).astype(int).fillna(0)

    # Agrupa total de leitos enfermaria
    df_cnes["number_beds"] = (
        df_cnes["cirurgico_tot"] + df_cnes["clinico_tot"] + df_cnes["hospital_dia_tot"]
    )

    # Agrupa total de leitos UTI
    df_cnes["number_icu_beds"] = (
        df_cnes["UTI_adulto_I_tot"]
        + df_cnes["UTI_adulto_II_tot"]
        + df_cnes["UTI_adulto_III_tot"]
    )

    # Agrupa total de leitos UTI Covid
    df_cnes["number_covid_icu_beds"] = (
        df_cnes["UTI_adulto_II_COVID_SUS"]
        + df_cnes["UTI_adulto_II_COVID_nao_SUS"]
        + df_cnes["UTI_pediatrica_II_COVID_SUS"]
        + df_cnes["UTI_pediatrica_II_COVID_nao_SUS"]
    )

    # Da merge com os dados de populacao
    places_ids = get_places_id.now(config)

    # Cria coluna de IBGE 6 dígitos para match
    places_ids["city_id_7d"] = places_ids["city_id"]
    places_ids["city_id"] = places_ids["city_id"]
    places_ids["city_id"] = places_ids["city_id"].astype(str).apply(lambda x: x[:-1])

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
            "UTI_pediatrica_II_COVID_nao_SUS",
            "city_name_y",
            "pediatrico_tot",
            "UTI_adulto_II_COVID_SUS",
            "UTI_pediatrica_II_COVID_SUS",
            "UTI_adulto_II_COVID_nao_SUS",
            "state_id_y",
            "cirurgico_tot",
            "clinico_tot",
            "hospital_dia_tot",
            "UTI_adulto_I_tot",
            "UTI_adulto_II_tot",
            "UTI_adulto_III_tot",
        ],
        axis=1,
    )

    # Preenche zero recursos para cidades com NaN
    resources = [
        "number_icu_beds",
        "number_beds",
        "number_covid_icu_beds",
        "number_ventilators",
    ]
    df_cnes[resources] = df_cnes[resources].fillna(0)

    # todayday = datetime.now().strftime("%Y-%m-%d")
    (
        df_cnes["last_updated_number_ventilators"],
        df_cnes["last_updated_number_beds"],
        df_cnes["last_updated_number_icu_beds"],
        df_cnes["last_updated_number_covid_icu_beds"],
        df_cnes["author_number_beds"],
        df_cnes["author_number_ventilators"],
        df_cnes["author_number_icu_beds"],
    ) = (
        updatedate,
        updatedate,
        updatedate,
        updatedate,
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
