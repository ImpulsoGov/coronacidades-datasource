from endpoints import get_health_region_parameters
from endpoints import get_places_id
import pandas as pd

from endpoints.helpers import allow_local


@allow_local
def now(config):
    # Parameters used on health region level for now!
    params = get_health_region_parameters.gen_stratified_parameters(
        config, "health_region_id"
    )

    return params.merge(
        get_places_id.now(config)[["health_region_id", "city_id"]],
        on="health_region_id",
    )


TESTS = {
    "df is not pd.DataFrame": lambda df: isinstance(df, pd.DataFrame),
    "dataframe has null data": lambda df: all(df.isnull().any() == False),
}
