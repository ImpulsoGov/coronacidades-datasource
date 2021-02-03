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
from endpoints import get_states_farolcovid_main, get_cities_farolcovid_main
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
import yaml
from plotly.offline import download_plotlyjs, plot, iplot
import random

from datawrapper import Datawrapper

import os


class Map:
    def __init__(
        self, config, map_folder_id, access_token=None, basemapCMD=None, state_id=None
    ):

        self.config = config
        self.map_folder_id = map_folder_id
        self.dw = Datawrapper(access_token)
        self.basemapCMD = basemapCMD
        self.state_id = state_id

        # __dadosFarol__
        self.map_data = self.__dadosFarol__()

    def __dadosFarol__(self):
        # Puxa os dados do Farol

        if self.state_id:
            data = (
                get_cities_farolcovid_main.now(self.config)
                .query(f"state_id == '{self.state_id}'")[
                    [
                        "city_id",
                        "city_name",
                        "overall_alert",
                        "deaths",
                        "subnotification_rate",
                    ]
                ]
                .rename(columns={"city_id": "ID"})
            )
        else:
            data = (
                get_states_farolcovid_main.now(self.config)
                .sort_values("state_id")
                .reset_index(drop=True)[
                    [
                        "state_id",
                        "state_name",
                        "overall_alert",
                        "deaths",
                        "subnotification_rate",
                    ]
                ]
                .rename(columns={"state_id": "ID"})
            )

        data = data.assign(
            Value=lambda df: df["overall_alert"].fillna(-1),
            overall_alert=lambda df: df["overall_alert"]
            .map(self.config["br"]["farolcovid"]["categories"])
            .fillna("-"),
        )

        return data

    def createMap(self):
        # Cria o Mapa
        stateMap = self.dw.create_chart(
            title=" ",
            chart_type="d3-maps-choropleth",
            data=self.map_data,
            folder_id=self.map_folder_id,
        )

        if self.state_id:
            self.dw.add_data(stateMap["publicId"], self.map_data)

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

        # Colunas com dados a serem mostrados no hover
        fields = {
            "ID": "ID",
            "Value": "Value",
            "deaths": "deaths",
            "overall_alert": "overall_alert",
            "subnotification_rate": "subnotification_rate",
        }

        if self.state_id:
            map_key_attr = "CD_GEOCMU"
            title = "{{ city_name }}"
            fields["city_name"] = "city_name"

        else:
            map_key_attr = "postal"
            title = "{{ state_name }}"
            fields["state_name"] = "state_name"

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
                    "body": """<p>Alerta: {{ overall_alert }}</p>
                    <p>Total de Mortes: {{ deaths }}</p>
                    <p>Subnotificação: {{ subnotification_rate }}</p>""",
                    "title": title,
                    "fields": fields,
                },
                "map-key-attr": map_key_attr,
                "map-key-auto": False,
                "map-type-set": "true",
                "gradient": {
                    "stops": [{"p": 0, "v": -2}, {"p": 1, "v": 4}],
                    "colors": [
                        {"c": "#c4c4c4", "p": 0},
                        {"c": "#c4c4c4", "p": 1 / 6},  # v = -1 (null)
                        {"c": "#0990A7", "p": 2 / 6},  # v = 0 (novo normal)
                        {"c": "#F7B502", "p": 3 / 6},  # v = 1 (moderado)
                        {"c": "#F77800", "p": 4 / 6},  # v = 2 (alto)
                        {"c": "#F22E3E", "p": 5 / 6},  # v = 3 (altissimo)
                        {"c": "#F22E3E", "p": 1},
                    ],
                    # "domain": [0, 0.2, 0.4, 0.6, 0.8],
                },
            },
        }

        self.dw.update_metadata(mapID, DEFAULT_LAYOUT)

    def updateMap(self, mapID):
        # Read farol data
        self.dw.add_data(mapID, self.map_data)
        # Update layout
        self.applyDefaultLayout(mapID)
        # Update chart on DW
        self.dw.update_chart(mapID, title="")


@allow_local
def now(config):
    """This method is going to be called by main.py and it should return the output
    DataFrame.

    Parameters
    ----------
    config : dict
    """

    # INIT
    idStateCode = config["br"]["maps"]["idStateCode"]
    idStatesMap = config["br"]["maps"]["idStatesMap"]
    states = idStatesMap.keys()
    ACCESS_TOKEN = os.getenv("MAP_ACCESS_TOKEN")

    if None in idStatesMap.values():
        IS_DEV = True
        print("Generating new ids")
    else:
        IS_DEV = os.getenv("IS_MAP_DEV") == "True"

    if not IS_DEV:
        map_folder_id = config["br"]["maps"]["MAP_FOLDER_ID"]
    else:
        map_folder_id = 38060  # "maps-coronacidades"
        
    dw = Datawrapper(access_token="q90ztm8IzbSpa6YfwN190rxQnQROdmRI2cg76QHLEFONF39QteI4mjTxWgL8xuza")
#     dw = Datawrapper(access_token=ACCESS_TOKEN)

    if IS_DEV:
        # Create states map
        for state_id in states:
            config["br"]["maps"]["idStatesMap"][state_id] = Map(
                config,
                map_folder_id,
                ACCESS_TOKEN,
                basemapCMD=f"brazil-{idStateCode[state_id]}-municipalities",
                state_id=state_id,
            ).createMap()

            # Update layout
            Map(
                config,
                map_folder_id,
                ACCESS_TOKEN,
                basemapCMD=f"brazil-{idStateCode[state_id]}-municipalities",
                state_id=state_id,
            ).applyDefaultLayout(config["br"]["maps"]["idStatesMap"][state_id])

            dw.publish_chart(config["br"]["maps"]["idStatesMap"][state_id])
            print(state_id + ": " + config["br"]["maps"]["idStatesMap"][state_id])

        # Create country map
        config["br"]["maps"]["BR_ID"] = Map(
            config,
            map_folder_id,
            ACCESS_TOKEN,
            basemapCMD="brazil-states-2018",
            state_id=None,
        ).createMap()

        # Update layout
        Map(
            config,
            map_folder_id,
            ACCESS_TOKEN,
            basemapCMD="brazil-states-2018",
            state_id=None,
        ).applyDefaultLayout(config["br"]["maps"]["BR_ID"])

        dw.publish_chart(config["br"]["maps"]["BR_ID"])
        print("BR : " + config["br"]["maps"]["BR_ID"])

    else:
        # Update states map
        for state_id in states:
            Map(
                config,
                map_folder_id,
                ACCESS_TOKEN,
                basemapCMD=f"brazil-{idStateCode[state_id]}-municipalities",
                state_id=state_id,
            ).updateMap(config["br"]["maps"]["idStatesMap"][state_id])

            dw.publish_chart(config["br"]["maps"]["idStatesMap"][state_id])
            print(state_id + ": " + config["br"]["maps"]["idStatesMap"][state_id])

        # Update country map
        Map(
            config,
            map_folder_id,
            ACCESS_TOKEN,
            basemapCMD="brazil-states-2018",
            state_id=None,
        ).updateMap(config["br"]["maps"]["BR_ID"])

        dw.publish_chart(config["br"]["maps"]["BR_ID"])
        print("BR : " + config["br"]["maps"]["BR_ID"])

    out_frame = (
        pd.DataFrame(
            {
                "place_id": list(config["br"]["maps"]["idStatesMap"].keys()),
                "map_id": list(config["br"]["maps"]["idStatesMap"].values()),
            }
        )
        .append(
            pd.DataFrame({"place_id": "BR", "map_id": [config["br"]["maps"]["BR_ID"]]})
        )
        .reset_index(drop=True)
    )

    # Gens the hashes for version control
    out_frame["hashes"] = [
        "".join(random.choice("0123456789ABCDEF") for i in range(16))
        for i in range(out_frame.shape[0])
    ]
    return out_frame


# Output dataframe tests to check data integrity. This is also going to be called
# by main.py
TESTS = {
    "dataframe has null data": lambda df: all(df.isnull().any() == False),
    "not all states + country maps ids saved in config": lambda df: len(df) == 28,
}

