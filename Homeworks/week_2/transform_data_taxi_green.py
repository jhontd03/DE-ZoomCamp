import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    print(sum([1 if 'ID' in col else 0 for col in data.columns]))

    data.columns = [col.replace('ID', '_id') if 'ID' in col else col for col in data.columns]
    data.columns = data.columns.str.lower()

    non_zero_psg_trip = data[(data['passenger_count'] > 0) & (data['trip_distance'] > 0) ]
        
    print(non_zero_psg_trip[['vendor_id']].value_counts())

    return non_zero_psg_trip


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    # assert output is not None, 'The output is undefined'
    assert output['passenger_count'].isin([0]).sum() == 0
    assert output['trip_distance'].isin([0]).sum() == 0
    assert output[['vendor_id']].value_counts().index[0][0] == 2
