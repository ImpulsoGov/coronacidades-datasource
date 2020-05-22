import subprocess
import unicodedata
import tempfile
import pandas as pd
import yaml
import os
import requests
import numpy as np


def build_file_path(endpoint):

    if endpoint["endpoint"] == "secret":

        route = secrets(endpoint["secret_path"])["route"]
    else:
        route = endpoint["endpoint"]

    fn = route.replace("/", "-")

    return "/".join([os.getenv("OUTPUT_DIR"), fn]) + ".csv"


def _remove_accents(text):
    return (
        unicodedata.normalize("NFKD", text)
        .encode("ASCII", "ignore")
        .decode("ASCII")
        .upper()
    )


def _drop_forbiden(text):

    forbiden = [
        "AC",
        "AL",
        "AM",
        "AP",
        "BA",
        "CE",
        "DF",
        "ES",
        "GO",
        "MA",
        "MG",
        "MS",
        "MT",
        "PA",
        "PB",
        "PE",
        "PI",
        "PR",
        "RJ",
        "RN",
        "RO",
        "RR",
        "RS",
        "SC",
        "SE",
        "SP",
        "TO",
    ]

    words = [t.strip() for t in text.split(" ")]

    for f in forbiden:
        if f in words:
            words.remove(f)

    return " ".join(words)


def treat_text(s):

    s = s.apply(_remove_accents)
    s = s.apply(_drop_forbiden)
    return s


def get_last(_df, sort_by="last_updated"):

    return _df.sort_values(sort_by).groupby(["city_id"]).last().reset_index()


def download_from_drive(url):

    temp_path = tempfile.gettempdir() + "/temp.csv"

    response = subprocess.run(
        ["curl", "-sk", "-o", temp_path, url + "/export?format=csv&id"]
    )

    return pd.read_csv(temp_path)


def secrets(variable, path="secrets.yaml"):

    if (isinstance(variable, str)) and (os.getenv(variable)):
        return os.getenv(variable)
    else:
        if not isinstance(variable, list):
            variable = list(variable)

        secrets = yaml.load(open(path, "r"), Loader=yaml.FullLoader)

        for var in variable:
            secrets = secrets[var]

        return secrets


def get_config(url=os.getenv("CONFIG_URL")):

    return yaml.load(requests.get(url).text, Loader=yaml.FullLoader)


def get_endpoints():

    return yaml.load(open("endpoints.yaml", "r"), Loader=yaml.FullLoader)


def get_cases_series(df, place_type, min_days):

    df = (
        df[~df["state_notification_rate"].isnull()][
            [place_type, "last_updated", "active_cases"]
        ]
        .groupby([place_type, "last_updated"])["active_cases"]
        .sum()
        .round(0)
    )

    # more than 14 days
    v = df.reset_index()[place_type].value_counts()

    return df[df.index.isin(v[v > min_days].index, level=0)]


def get_country_isocode_name(iso):
    names = {
        "AFG": "Afeganistão",
        "ZAF": "África do Sul",
        "ALB": "Albânia",
        "DEU": "Alemanha",
        "AND": "Andorra",
        "AGO": "Angola",
        "AIA": "Anguilla",
        "ATA": "Antártida",
        "ATG": "Antígua e Barbuda",
        "ANT": "Antilhas Holandesas",
        "SAU": "Arábia Saudita",
        "DZA": "Argélia",
        "ARG": "Argentina",
        "ARM": "Armênia",
        "ABW": "Aruba",
        "AUS": "Austrália",
        "AUT": "Áustria",
        "AZE": "Azerbaijão",
        "BHS": "Bahamas",
        "BHR": "Bahrein",
        "BGD": "Bangladesh",
        "BRB": "Barbados",
        "BLR": "Belarus",
        "BEL": "Bélgica",
        "BLZ": "Belize",
        "BEN": "Benin",
        "BMU": "Bermudas",
        "BOL": "Bolívia",
        "BIH": "Bósnia-Herzegóvina",
        "BWA": "Botsuana",
        "BRA": "Brasil",
        "BRN": "Brunei",
        "BGR": "Bulgária",
        "BFA": "Burkina Fasso",
        "BDI": "Burundi",
        "BTN": "Butão",
        "CPV": "Cabo Verde",
        "CMR": "Camarões",
        "KHM": "Camboja",
        "CAN": "Canadá",
        "KAZ": "Cazaquistão",
        "TCD": "Chade",
        "CHL": "Chile",
        "CHN": "China",
        "CYP": "Chipre",
        "SGP": "Cingapura",
        "COL": "Colômbia",
        "COG": "Congo",
        "PRK": "Coréia do Norte",
        "KOR": "Coréia do Sul",
        "CIV": "Costa do Marfim",
        "CRI": "Costa Rica",
        "HRV": "Croácia (Hrvatska)",
        "CUB": "Cuba",
        "CUW": "Curaçao",
        "DNK": "Dinamarca",
        "DJI": "Djibuti",
        "DMA": "Dominica",
        "EGY": "Egito",
        "SLV": "El Salvador",
        "ARE": "Emirados Árabes Unidos",
        "ECU": "Equador",
        "ERI": "Eritréia",
        "SVK": "Eslováquia",
        "SVN": "Eslovênia",
        "ESP": "Espanha",
        "USA": "Estados Unidos",
        "EST": "Estônia",
        "ETH": "Etiópia",
        "FJI": "Fiji",
        "PHL": "Filipinas",
        "FIN": "Finlândia",
        "FRA": "França",
        "GAB": "Gabão",
        "GMB": "Gâmbia",
        "GHA": "Gana",
        "GEO": "Geórgia",
        "GIB": "Gibraltar",
        "GBR": "Reino Unido",
        "GRD": "Granada",
        "GRC": "Grécia",
        "GRL": "Groelândia",
        "GLP": "Guadalupe",
        "GUM": "Guam (Território dos Estados Unidos)",
        "GTM": "Guatemala",
        "GGY": "Guernsey",
        "GUY": "Guiana",
        "GUF": "Guiana Francesa",
        "GIN": "Guiné",
        "GNQ": "Guiné Equatorial",
        "GNB": "Guiné-Bissau",
        "HTI": "Haiti",
        "NLD": "Holanda",
        "HND": "Honduras",
        "HKG": "Hong Kong",
        "HUN": "Hungria",
        "YEM": "Iêmen",
        "BVT": "Ilha Bouvet (Território da Noruega)",
        "IMN": "Ilha do Homem",
        "CXR": "Ilha Natal",
        "PCN": "Ilha Pitcairn",
        "REU": "Ilha Reunião",
        "ALA": "Ilhas Aland",
        "CYM": "Ilhas Cayman",
        "CCK": "Ilhas Cocos",
        "COM": "Ilhas Comores",
        "COK": "Ilhas Cook",
        "FRO": "Ilhas Faroes",
        "FLK": "Ilhas Falkland (Malvinas)",
        "SGS": "Ilhas Geórgia do Sul e Sandwich do Sul",
        "HMD": "Ilhas Heard e McDonald (Território da Austrália)",
        "MNP": "Ilhas Marianas do Norte",
        "MHL": "Ilhas Marshall",
        "UMI": "Ilhas Menores dos Estados Unidos",
        "NFK": "Ilhas Norfolk",
        "SYC": "Ilhas Seychelles",
        "SLB": "Ilhas Solomão",
        "SJM": "Ilhas Svalbard e Jan Mayen",
        "TKL": "Ilhas Tokelau",
        "TCA": "Ilhas Turks e Caicos",
        "VIR": "Ilhas Virgens (Estados Unidos)",
        "VGB": "Ilhas Virgens (Inglaterra)",
        "WLF": "Ilhas Wallis e Futuna",
        "IND": "índia",
        "IDN": "Indonésia",
        "IRN": "Irã",
        "IRQ": "Iraque",
        "IRL": "Irlanda",
        "ISL": "Islândia",
        "ISR": "Israel",
        "ITA": "Itália",
        "JAM": "Jamaica",
        "JPN": "Japão",
        "JEY": "Jersey",
        "JOR": "Jordânia",
        "KEN": "Kênia",
        "KIR": "Kiribati",
        "KWT": "Kuait",
        "LAO": "Laos",
        "LVA": "Látvia",
        "LSO": "Lesoto",
        "LBN": "Líbano",
        "LBR": "Libéria",
        "LBY": "Líbia",
        "LIE": "Liechtenstein",
        "LTU": "Lituânia",
        "LUX": "Luxemburgo",
        "MAC": "Macau",
        "MKD": "Macedônia",
        "MDG": "Madagascar",
        "MYS": "Malásia",
        "MWI": "Malaui",
        "MDV": "Maldivas",
        "MLI": "Mali",
        "MLT": "Malta",
        "MAR": "Marrocos",
        "MTQ": "Martinica",
        "MUS": "Maurício",
        "MRT": "Mauritânia",
        "MYT": "Mayotte",
        "MEX": "México",
        "FSM": "Micronésia",
        "MOZ": "Moçambique",
        "MDA": "Moldova",
        "MCO": "Mônaco",
        "MNG": "Mongólia",
        "MNE": "Montenegro",
        "MSR": "Montserrat",
        "MMR": "Myanma",
        "NAM": "Namíbia",
        "NRU": "Nauru",
        "NPL": "Nepal",
        "NIC": "Nicarágua",
        "NER": "Níger",
        "NGA": "Nigéria",
        "NIU": "Niue",
        "NOR": "Noruega",
        "NCL": "Nova Caledônia",
        "NZL": "Nova Zelândia",
        "OMN": "Omã",
        "BES": "Países Baixo do Caribe",
        "PLW": "Palau",
        "PAN": "Panamá",
        "PNG": "Papua-Nova Guiné",
        "PAK": "Paquistão",
        "PRY": "Paraguai",
        "PER": "Peru",
        "PYF": "Polinésia Francesa",
        "POL": "Polônia",
        "PRI": "Porto Rico",
        "PRT": "Portugal",
        "QAT": "Qatar",
        "KGZ": "Quirguistão",
        "CAF": "República Centro-Africana",
        "COD": "República Democrática do Congo",
        "DOM": "República Dominicana",
        "CZE": "República Tcheca",
        "ROU": "Romênia",
        "RWA": "Ruanda",
        "RUS": "Rússia",
        "ESH": "Saara Ocidental",
        "VCT": "Saint Vincente e Granadinas",
        "ASM": "Samoa Americana",
        "WSM": "Samoa Ocidental",
        "SMR": "San Marino",
        "SHN": "Santa Helena",
        "LCA": "Santa Lúcia",
        "BLM": "São Bartolomeu",
        "KNA": "São Cristóvão e Névis",
        "MAF": "São Martim",
        "SXM": "São Martinhno",
        "STP": "São Tomé e Príncipe",
        "SEN": "Senegal",
        "SLE": "Serra Leoa",
        "SRB": "Sérvia",
        "SYR": "Síria",
        "SOM": "Somália",
        "LKA": "Sri Lanka",
        "SPM": "St. Pierre and Miquelon",
        "SWZ": "Suazilândia",
        "SDN": "Sudão",
        "SSD": "Sudão do Sul",
        "SWE": "Suécia",
        "CHE": "Suíça",
        "SUR": "Suriname",
        "TJK": "Tadjiquistão",
        "THA": "Tailândia",
        "TWN": "Taiwan",
        "TZA": "Tanzânia",
        "IOT": "Território Britânico do Oceano índico",
        "ATF": "Territórios do Sul da França",
        "PSE": "Territórios Palestinos Ocupados",
        "TLS": "Timor Leste",
        "TGO": "Togo",
        "TON": "Tonga",
        "TTO": "Trinidad and Tobago",
        "TUN": "Tunísia",
        "TKM": "Turcomenistão",
        "TUR": "Turquia",
        "TUV": "Tuvalu",
        "UKR": "Ucrânia",
        "UGA": "Uganda",
        "URY": "Uruguai",
        "UZB": "Uzbequistão",
        "VUT": "Vanuatu",
        "VAT": "Vaticano",
        "VEN": "Venezuela",
        "VNM": "Vietnã",
        "ZMB": "Zâmbia",
        "ZWE": "Zimbábue",
    }

    if isinstance(iso, pd.Series):
        return [get_country_isocode_name(x) for x in iso]

    if iso in names.keys():
        return names[iso]
    else:
        return np.nan
