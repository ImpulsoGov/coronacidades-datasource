from endpoints.helpers import allow_local
import numpy as np
import pandas as pd

pd.options.display.max_columns = 999

import warnings

warnings.filterwarnings("ignore")

# Plotting
import plotly
import plotly.graph_objs as go
import cufflinks as cf

plotly.offline.init_notebook_mode(connected=True)
# Getting helping data
# from endpoints import get_states_farolcovid_main, get_cities_farolcovid_main
import utils

# Setting cufflinks
import textwrap
import cufflinks as cf

cf.go_offline()
cf.set_config_file(offline=False, world_readable=True)

# Centering and fixing title
def iplottitle(title, width=40):
    return "<br>".join(textwrap.wrap(title, width))


# import geobr
#%matplotlib inline
import yaml
from plotly.offline import download_plotlyjs, plot, iplot
import random

# import geopandas as gpd
# import webbrowser
from datawrapper import Datawrapper

import os

# INIT
config = utils.get_config()
dictsDW = yaml.load(open("map_config.yaml", "r"), Loader=yaml.FullLoader)
idStateCode = dictsDW["idStateCode"]
idStatesMap = dictsDW["idStatesMap"]
IS_DEV = os.getenv("IS_MAP_DEV") == "True"
if IS_DEV != True and IS_DEV != False:
    IS_DEV = True
if None in idStatesMap.values():
    IS_DEV = True
    print("Generating new ids")
states = idStatesMap.keys()
ACCESS_TOKEN = os.getenv("MAP_ACCESS_TOKEN")
if not IS_DEV:
    MAP_FOLDER_ID = dictsDW["MAP_FOLDER_ID"]
else:
    MAP_FOLDER_ID = "maps-coronacidades"
dw = Datawrapper(access_token=ACCESS_TOKEN)
# CLASSES
class StateMap:
    def __init__(self, state_id, acess_token=None):
        self.state_id = state_id
        self.dw = Datawrapper(acess_token)

        # __dadosFarol__
        self.mapData, self.farolcovidState = self.__dadosFarol__()
        self.date = pd.to_datetime(self.farolcovidState["last_updated_rt"][0]).strftime(
            "%d/%m/%Y"
        )

        # __getCodes__
        self.main_title, self.basemapCMD = self.__getCodes__()

        # createMap

    def __dadosFarol__(self):
        # Puxa os dados do Farol
        # farolcovid_cities = get_cities_farolcovid_main.now(config)
        farolcovid_cities = pd.read_csv(
            "http://datasource.coronacidades.org/br/cities/farolcovid/main"
        )
        farolcovid_cities = farolcovid_cities.sort_values(
            ["state_id", "city_name"]
        ).reset_index(drop=True)

        farolcovidState = (
            farolcovid_cities.query(f"state_id == '{self.state_id}'")
            .sort_values("city_id")
            .reset_index(drop=True)
        )

        mapData = farolcovidState[
            ["city_id", "city_name", "overall_alert", "deaths", "subnotification_rate"]
        ]
        mapData.columns = [
            "ID",
            "city_name",
            "overall_alert",
            "deaths",
            "subnotification_rate",
        ]
        mapData["Value"] = mapData["overall_alert"].apply(
            lambda x: 29
            if x == "alto"
            else (0 if x == "medio" else (-29 if x == "baixo" else np.nan))
        )

        return mapData, farolcovidState

    def __getCodes__(self):
        # Pega o código do mapa para criar o Mapa por estado
        state_name = self.farolcovidState["state_name"][0]
        main_title = f"{state_name}<br>Fonte: Impulso | {self.date}"
        basemapcode = idStateCode[self.state_id]
        basemapCMD = f"brazil-{basemapcode}-municipalities"
        return main_title, basemapCMD

    def createMap(self):
        # Cria o Mapa
        stateMap = self.dw.create_chart(
            title=self.main_title,
            chart_type="d3-maps-choropleth",
            folder_id=MAP_FOLDER_ID,
        )
        self.dw.add_data(stateMap["publicId"], self.mapData)
        mapContour = {
            "axes": {"keys": "ID", "values": "Value"},
            "publish": {
                "embed-width": 600,
                "chart-height": 653.3333740234375,
                "embed-height": 723,
            },
            "visualize": {"basemap": f"{self.basemapCMD}"},
        }

        self.dw.update_metadata(stateMap["publicId"], mapContour)
        self.dw.update_chart(stateMap["publicId"], theme="datawrapper")
        return stateMap["publicId"]

    def applyDefaultLayout(self, mapID):
        # Aplica o layout
        DEFAULT_LAYOUT = {
            "data": {
                "transpose": False,
                "column-format": {
                    "ID": {
                        "type": "text",
                        "ignore": False,
                        "number-append": "",
                        "number-format": "auto",
                        "number-divisor": 0,
                        "number-prepend": "",
                    }
                },
            },
            "visualize": {
                "tooltip": {
                    "body": """<p>Risco: {{ overall_alert }}</p>
            <p>Total de Mortes: {{ deaths }}</p>
            <p>Subnotificação: {{ subnotification_rate }}</p>""",  # <a href={{ link }} target="_blank" rel="noreferrer nofollow">Mostrar Mais!</a>,
                    "title": "{{ city_name }}",
                    "fields": {
                        "ID": "ID",  # Alterar 'fields' se forem adicionadas outras colunas ao dataframe dentro dessa Classe
                        "Value": "Value",
                        "deaths": "deaths",
                        "city_name": "city_name",
                        "overall_alert": "overall_alert",
                        "subnotification_rate": "subnotification_rate",
                    },
                },
                "map-key-attr": "CD_GEOCMU",
                "map-key-auto": False,
                "map-type-set": "true",
                "gradient": {
                    "stops": [{"p": 0, "v": -30}, {"p": 1, "v": 30}],
                    "colors": [
                        {"c": "#0097A7", "p": 0},
                        {"c": "#0097A7", "p": 0.33},
                        {"c": "#F2C94C", "p": 0.33001},
                        {"c": "#F2C94C", "p": 0.66},
                        {"c": "#FF5F6B", "p": 0.66001},
                        {"c": "#FF5F6B", "p": 1},
                    ],
                    "domain": [0, 0.33001, 0.66001],
                },
            },
        }
        self.dw.update_metadata(mapID, DEFAULT_LAYOUT)

    def updateMap(self, mapID):
        self.dw.add_data(mapID, self.mapData)
        self.applyDefaultLayout(mapID)
        self.dw.update_chart(mapID, title=self.main_title)


class BrMap:
    def __init__(self, acess_token=None):
        self.dw = Datawrapper(acess_token)

        # __dadosFarol__
        self.mapData, self.farolcovid_states = self.__dadosFarol__()
        self.date = pd.to_datetime(
            self.farolcovid_states["last_updated_rt"][0]
        ).strftime("%d/%m/%Y")

        # __getCodes__
        self.main_title = self.__getCodes__()

        # createMap

    def __dadosFarol__(self):
        # Puxa os dados do Farol
        # farolcovid_states = get_states_farolcovid_main.now(config)
        farolcovid_states = pd.read_csv(
            "http://datasource.coronacidades.org/br/states/farolcovid/main"
        )
        farolcovid_states = farolcovid_states.sort_values("state_id").reset_index(
            drop=True
        )

        mapData = farolcovid_states[
            [
                "state_id",
                "state_name",
                "overall_alert",
                "deaths",
                "subnotification_rate",
            ]
        ]
        mapData.columns = [
            "ID",
            "state_name",
            "overall_alert",
            "deaths",
            "subnotification_rate",
        ]
        mapData["Value"] = mapData["overall_alert"].apply(
            lambda x: 29
            if x == "alto"
            else (0 if x == "medio" else (-29 if x == "baixo" else np.nan))
        )

        return mapData, farolcovid_states

    def __getCodes__(self):
        # Pega o código do mapa para criar o Mapa por estado
        main_title = f"FarolCovid - Estados da União<br>Fonte: Impulso | {self.date}"
        return main_title

    def createMap(self):
        # Cria o Mapa
        stateMap = self.dw.create_chart(
            title=self.main_title,
            chart_type="d3-maps-choropleth",
            data=self.mapData,
            folder_id=MAP_FOLDER_ID,
        )
        mapContour = {
            "axes": {"keys": "ID", "values": "Value"},
            "publish": {
                "embed-width": 600,
                "chart-height": 653.3333740234375,
                "embed-height": 723,
            },
            "visualize": {"basemap": "brazil-states-2018"},
        }

        self.dw.update_metadata(stateMap["publicId"], mapContour)
        self.dw.update_chart(stateMap["publicId"], theme="datawrapper")
        return stateMap["publicId"]

    def applyDefaultLayout(self, mapID):
        # Aplica o layout
        DEFAULT_LAYOUT = {
            "data": {
                "transpose": False,
                "column-format": {
                    "ID": {
                        "type": "text",
                        "ignore": False,
                        "number-append": "",
                        "number-format": "auto",
                        "number-divisor": 0,
                        "number-prepend": "",
                    }
                },
            },
            "visualize": {
                "tooltip": {
                    "body": """
   <p>Risco: {{ overall_alert }}</p>
   <p>Total de Mortes: {{ deaths }}</p>
   <p>Subnotificação: {{ subnotification_rate }}</p>""",
                    "title": "{{ state_name }}",
                    "fields": {
                        "ID": "ID",  # Alterar 'fields' se forem adicionadas outras colunas ao dataframe dentro dessa Classe
                        "Value": "Value",
                        "deaths": "deaths",
                        "state_name": "state_name",
                        "overall_alert": "overall_alert",
                        "subnotification_rate": "subnotification_rate",
                    },
                },
                "map-key-attr": "postal",
                "map-key-auto": False,
                "map-type-set": "true",
                "gradient": {
                    "stops": [{"p": 0, "v": -30}, {"p": 1, "v": 30}],
                    "colors": [
                        {"c": "#0097A7", "p": 0},
                        {"c": "#0097A7", "p": 0.33},
                        {"c": "#F2C94C", "p": 0.33001},
                        {"c": "#F2C94C", "p": 0.66},
                        {"c": "#FF5F6B", "p": 0.66001},
                        {"c": "#FF5F6B", "p": 1},
                    ],
                    "domain": [0, 0.33001, 0.66001],
                },
            },
        }

        self.dw.update_metadata(mapID, DEFAULT_LAYOUT)

    def updateMap(self, mapID):
        self.dw.add_data(mapID, self.mapData)
        self.applyDefaultLayout(mapID)
        self.dw.update_chart(mapID, title=self.main_title)


@allow_local
def now(config):
    """This method is going to be called by main.py and it should return the output
    DataFrame.

    Parameters
    ----------
    config : dict
    """
    pass
    if IS_DEV:
        # Gen states
        for state in states:
            dictsDW["idStatesMap"][state] = StateMap(state, ACCESS_TOKEN).createMap()
            StateMap(state, ACCESS_TOKEN).applyDefaultLayout(
                dictsDW["idStatesMap"][state]
            )
            dw.publish_chart(dictsDW["idStatesMap"][state])
            print(state + ": " + dictsDW["idStatesMap"][state])
        # Gen country
        dictsDW["BR_ID"] = BrMap(ACCESS_TOKEN).createMap()
        BrMap(ACCESS_TOKEN).applyDefaultLayout(dictsDW["BR_ID"])
        dw.publish_chart(dictsDW["BR_ID"])
        print("BR : " + dictsDW["BR_ID"])
    else:
        # Gen states
        for state in states:
            StateMap(state, ACCESS_TOKEN).updateMap(dictsDW["idStatesMap"][state])
            dw.publish_chart(dictsDW["idStatesMap"][state])
            print(state + ": " + dictsDW["idStatesMap"][state])
        # Gen country
        BrMap(ACCESS_TOKEN).updateMap(dictsDW["BR_ID"])
        dw.publish_chart(dictsDW["BR_ID"])
        print("BR : " + dictsDW["BR_ID"])

    out_frame = pd.DataFrame(
        {
            "place_id": list(dictsDW["idStatesMap"].keys()),
            "map_id": list(dictsDW["idStatesMap"].values()),
        }
    )
    out_frame = out_frame.append(
        pd.DataFrame({"place_id": "BR", "map_id": [dictsDW["BR_ID"]]})
    )
    out_frame.reset_index()
    # gens the hashes for version control
    out_frame["hashes"] = [
        "".join(random.choice("0123456789ABCDEF") for i in range(16))
        for i in range(out_frame.shape[0])
    ]
    return out_frame


# Output dataframe tests to check data integrity. This is also going to be called
# by main.py
TESTS = {
    "dataframe has null data": lambda df: all(df.isnull().any() == False),
}
