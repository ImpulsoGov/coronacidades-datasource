import pandas as pd
from endpoints.helpers import allow_local
from endpoints import get_inloco_cities


@allow_local
def now(config):
    return get_inloco_cities.now(config).query("state_num_id == 43")


TESTS = {
    "data is not pd.DataFrame": lambda df: isinstance(df, pd.DataFrame),
    "dataframe has null data": lambda df: all(df.isnull().any() == False),
}
