from fin_elt.elt.extract import Extract

def test_api_call():
    output = Extract.crypto_price('BTC', 'USD', api_key)
    assert output.shape[0] > 0