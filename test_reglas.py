import pandas as pd
from src.calidad.reglas import aplicar_reglas

def test_humedad_fuera_de_rango():
    df = pd.DataFrame({"h": [50.0, 120.0]})
    assert aplicar_reglas(df, "meteo").flag.tolist() == [0, 1]

def test_vvmax_menor_que_vv():
    df = pd.DataFrame({"vv": [2.0, 5.0], "vv_max": [3.0, 4.0]})
    assert aplicar_reglas(df, "meteo").flag.tolist() == [0, 1]

def test_salto_de_nivel():
    df = pd.DataFrame({"nivel": [7.0, 7.1, 20.0]})
    assert aplicar_reglas(df, "nivel").flag.tolist()[2] == 1

def test_meseta_congelada():
    df = pd.DataFrame({"nivel": [5.0] + [18.1] * 12})
    assert aplicar_reglas(df, "nivel").flag.tolist()[-1] == 1

def test_radiacion_nocturna():
    df = pd.DataFrame({"fecha_hora": pd.to_datetime(["2026-04-30 20:22", "2026-04-30 12:00"]),
                       "radiacion": [482.9, 800.0]})
    assert aplicar_reglas(df, "piran").flag.tolist() == [1, 0]

def test_discrepancia_canales_pluvio():
    df = pd.DataFrame({"p1": [0.254, 0.0], "p2": [0.0, 0.0]})
    assert aplicar_reglas(df, "pluvio").flag.tolist() == [1, 0]

def test_fila_limpia_sin_banderas():
    df = pd.DataFrame({"h": [80.0], "t": [20.0], "pr": [815.0],
                       "vv": [1.0], "vv_max": [2.0], "dv": [90.0], "p": [0.0]})
    assert aplicar_reglas(df, "meteo").flag.tolist() == [0]