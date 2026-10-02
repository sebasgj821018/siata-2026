"""Salidas versionables: el crudo nunca se toca (E3)."""
import json, pathlib, datetime
import pandas as pd
OUT = pathlib.Path("outputs")

def guardar(df: pd.DataFrame, nombre: str, run_id: str):
    OUT.mkdir(exist_ok=True)
    ruta = OUT / f"{nombre}.csv"
    df.to_csv(ruta, index=False)
    man = OUT / "manifest.json"
    reg = {"run_id": run_id, "tabla": nombre, "filas": int(len(df)),
           "ts": datetime.datetime.now(datetime.timezone.utc).isoformat()}
    hist = json.loads(man.read_text()) if man.exists() else []
    hist.append(reg); man.write_text(json.dumps(hist, indent=2))
    return ruta