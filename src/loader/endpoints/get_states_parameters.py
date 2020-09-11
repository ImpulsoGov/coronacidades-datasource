from endpoints import get_health_region_parameters
import pandas as pd

from endpoints.helpers import allow_local


@allow_local
def now(config):
    return get_health_region_parameters.gen_stratified_parameters(
        config, "state_num_id"
    )


TESTS = {
    "df is not pd.DataFrame": lambda df: isinstance(df, pd.DataFrame),
    "dataframe has null data": lambda df: all(df.isnull().any() == False),
}
