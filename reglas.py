"""Reglas deterministas de calidad por tipo de sensor (Tarea C1)."""
import pandas as pd

def reglas_meteo(df):
    f = pd.DataFrame(index=df.index)
    f["h_fuera"]   = (df.h < 0) | (df.h > 100)
    f["t_fuera"]   = (df.t < -5) | (df.t > 40)
    f["pr_fuera"]  = (df.pr < 700) | (df.pr > 900)
    f["vvmax_lt_vv"] = df.vv_max < df.vv
    f["dv_fuera"]  = (df.dv < 0) | (df.dv > 360)
    f["p_neg"]     = df.p < 0
    return f.fillna(False)

def reglas_nivel(df):
    f = pd.DataFrame(index=df.index)
    f["salto"]   = df.nivel.diff().abs() > 0.5
    f["meseta"]  = df.nivel.diff().eq(0).rolling(10).sum() >= 10
    f["neg"]     = df.nivel < 0
    f["fuera"]   = (df.nivel < -2) | (df.nivel > 30)
    return f.fillna(False)

def reglas_piran(df):
    noche = df.fecha_hora.dt.hour.isin(list(range(19, 24)) + list(range(0, 6)))
    f = pd.DataFrame(index=df.index)
    f["noche"]  = (df.radiacion > 50) & noche
    f["neg"]    = df.radiacion < 0
    f["alta"]   = df.radiacion > 1300
    return f.fillna(False)

def reglas_pluvio(df):
    f = pd.DataFrame(index=df.index)
    f["neg"]  = (df.p1 < 0) | (df.p2 < 0)
    f["disc"] = (df.p1 - df.p2).abs() > 0.254
    return f.fillna(False)

_DISP = {"meteo": reglas_meteo, "nivel": reglas_nivel,
         "piran": reglas_piran, "pluvio": reglas_pluvio}

def aplicar_reglas(df, tipo):
    """Devuelve DataFrame de banderas + columna `flag` (1 = al menos una regla)."""
    f = _DISP[tipo](df)
    f["flag"] = f.any(axis=1).astype(int)
    return f