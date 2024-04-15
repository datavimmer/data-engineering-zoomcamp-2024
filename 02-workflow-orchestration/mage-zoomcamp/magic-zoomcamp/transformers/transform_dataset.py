from mage_ai.data_cleaner.transformer_actions.base import BaseAction
from mage_ai.data_cleaner.transformer_actions.constants import ActionType, Axis
from mage_ai.data_cleaner.transformer_actions.utils import build_transformer_action
from pandas import DataFrame

import re


if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

def needs_snake_case(name):
    # Check if name contains uppercase letters not at the start or has spaces
    return not re.match(r'^[a-z]+(_[a-z]+)*$', name)



@transformer
def transform_dataset(df: DataFrame, *args, **kwargs) -> DataFrame:
    # Find indices of rows to remove
    invalid_rows_indices = df.index[(df['passenger_count'].isna()) | (df['passenger_count'] == 0.0) | (df['trip_distance'] == 0.0)].tolist()
    df.drop(invalid_rows_indices, axis=0, inplace=True)

    # Build and execute the REMOVE action
    """
    action = build_transformer_action(
        df,
        action_type=ActionType.REMOVE,
        axis=Axis.ROW,
        options={'rows': invalid_rows_indices},
    )
    """
    
    #df = BaseAction(action).execute(df)
    # Count columns that need to be renamed to snake case
    columns_to_rename = sum(needs_snake_case(col) for col in df.columns)
    print(columns_to_rename)
    df['lpep_pickup_date'] = df['lpep_pickup_datetime'].dt.date

    df.columns = (df.columns
                .str.replace('(?<=[a-z])(?=[A-Z])', '_', regex=True)
                .str.lower()
             )

    return df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

    assert 'vendor_id' in output.columns, "vendor_id data is missing"
    assert (output['passenger_count'] > 0).all(), "Invalid passenger_count values"
    assert (output['trip_distance'] > 0).all(), "Invalid trip_distance values"
