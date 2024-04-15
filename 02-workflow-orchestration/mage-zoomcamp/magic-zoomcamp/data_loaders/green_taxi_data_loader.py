import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    base_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-{month}.csv.gz"
    months = ['10', '11', '12']
    dfs = []

    for month in months:
        url = base_url.format(month=month)
        print(url)
        df = pd.read_csv(url, compression="gzip", parse_dates=["lpep_pickup_datetime", "lpep_dropoff_datetime"])
        dfs.append(df)

    data = pd.concat(dfs, ignore_index=True)

    print(data.info())
    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
