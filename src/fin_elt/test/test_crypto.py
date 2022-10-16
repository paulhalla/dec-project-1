from fin_elt.elt.extract import Extract

def test_api_call():
    output = Extract.crypto_price('BTC', 'USD', "LEhcKWDf25QEVsLw8GIiLrpwv9cJHtLx36dWLNxt")
    assert output.shape[0] > 0