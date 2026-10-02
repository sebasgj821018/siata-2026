## E4 — Llevar el pipeline a producción (modelo híbrido, AWS)

**Flujo:** edge con buffer local (store-and-forward) → ingesta AWS (API GW +
Lambda) → S3 crudo WORM particionado por fecha/estación → validación en
Fargate ejecutando **este mismo paquete** `src/calidad` con reglas
versionadas (semver) → S3 validado en Parquet con banderas y `run_id` →
productos horarios → API FastAPI + datos abiertos vía CDN.

**Orquestación:** Airflow (MWAA) con DAGs idempotentes por
(codigo, fecha_hora, run_id); reproceso = re-ejecución determinista por
partición. **On-premise:** motor de alertas en tiempo real y espejo crítico
30–90 días (la alerta no depende de internet). **Repositorio de calidad:**
catálogo + diccionario de banderas + reglas + linaje (OpenLineage) + los tres
indicadores de P1.3 con semáforo y alerta por silencio >15 min.

**Seguridad:** IAM mínimo privilegio, KMS en reposo, TLS en tránsito,
Ley 1581/2012 y Ley 1712/2014. **Costos:** egress controlado con CDN +
Parquet comprimido + agregados horarios públicos (minutal solo por API con
cuota). **IaC/CI-CD:** Terraform + GitHub Actions (pytest → plan → apply).

**Migración 30/60/90:** 30 = pipeline contenerizado y S3 de calidad;
60 = orquestación, catálogo y monitoreo; 90 = API pública, recalibración
estacional y revisión humana de banderas 2/3.


```python
# ==== FIGURAS E5 — CELDA AUTOCONTENIDA (no depende del estado del kernel) ====
import os
import numpy as np, pandas as pd
import matplotlib.pyplot as plt
from io import StringIO
os.makedirs("outputs", exist_ok=True)

FILES = {
 "met_367":  ("Estacion_meteorologica_367_2025-12-01_2026-04-30.csv", ["h","t","pr","vv","vv_max","dv","dv_max","p"]),
 "niv_803":  ("Estacion_nivel_803_2025-12-01_2026-04-30.csv", ["nivel"]),
 "pir_6004": ("Estacion_piranometro_6004_2025-12-01_2026-04-30.csv", ["radiacion"]),
 "plu_35":   ("Estacion_pluviometrica_35_2025-12-01_2026-04-30.csv", ["p1","p2"]),
}
ESPERADO = 217440

def load(key):
    path, vals = FILES[key]
    raw = [l for l in open(path, encoding="utf-8", errors="replace").read().splitlines() if l.strip()]
    ncol = 2 + len(vals) + 1
    good = [l for l in raw if len(l.split(",")) == ncol]
    df = pd.read_csv(StringIO("\n".join(good)), header=None,
                     names=["codigo","fecha_hora"]+vals+["calidad"])
    df["fecha_hora"] = pd.to_datetime(df["fecha_hora"], errors="coerce")
    for c in vals+["calidad"]: df[c] = pd.to_numeric(df[c], errors="coerce")
    return df.sort_values("fecha_hora").reset_index(drop=True)

DATA = {k: load(k) for k in FILES}

# ---- Fig 1: completitud por estación ----
comp = pd.Series({k: 100*len(df)/ESPERADO for k, df in DATA.items()})
fig, ax = plt.subplots(figsize=(6,3))
comp.plot.bar(ax=ax, color="tab:blue")
ax.set_ylabel("% completitud"); ax.set_ylim(95, 100.5)
ax.set_title("Completitud minutal por estación (dic-25 a abr-26)")
for i, v in enumerate(comp.values): ax.text(i, v+0.08, f"{v:.2f}", ha="center", fontsize=8)
plt.tight_layout(); plt.savefig("outputs/fig1_completitud.png", dpi=150); plt.close()

# ---- Fig 2: nivel 803 con saltos y mesetas marcados ----
df = DATA["niv_803"]
m = ((df.nivel.diff().abs() > 0.5) | (df.nivel.diff().eq(0).rolling(10).sum() >= 10)).fillna(False)
fig, ax = plt.subplots(figsize=(9,3))
ax.plot(df.fecha_hora, df.nivel, lw=.4, color="0.6", label="nivel crudo")
ax.scatter(df.fecha_hora[m], df.nivel[m], s=5, color="crimson", label="flag regla (salto/meseta)")
ax.set_title("Nivel 803: saltos y mesetas detectados"); ax.legend(fontsize=8)
plt.tight_layout(); plt.savefig("outputs/fig2_nivel_flags.png", dpi=150); plt.close()

# ---- Fig 3: correlación cruzada lluvia → Δnivel (horaria) ----
hp = DATA["plu_35"].set_index("fecha_hora")[["p1"]].resample("1h").sum()
hn = DATA["niv_803"].set_index("fecha_hora")[["nivel"]].resample("1h").mean()
idx = hp.index.intersection(hn.index)
ll  = hp.p1.loc[idx].fillna(0)
nv  = hn.nivel.loc[idx].interpolate(limit=3).ffill().bfill()
dnv = nv.diff().fillna(0)
cc  = [ll.corr(dnv.shift(-L)) for L in range(7)]     # corr(lluvia_t, Δnivel_{t+L})
lag_opt = int(np.argmax(cc))
fig, ax = plt.subplots(figsize=(6,3))
ax.bar(range(7), cc, color="tab:green"); ax.axvline(lag_opt, color="k", ls="--")
ax.set_xlabel("lag (h)"); ax.set_ylabel("corr(lluvia, Δnivel)")
ax.set_title(f"Lag óptimo {lag_opt} h: tiempo de concentración de cuenca")
plt.tight_layout(); plt.savefig("outputs/fig3_xcorr_lluvia_nivel.png", dpi=150); plt.close()

print("Figuras guardadas:", sorted(os.listdir("outputs")))
```

    C:\Users\SEBAS GJ\AppData\Local\Temp\ipykernel_15716\68538246.py:21: DtypeWarning: Columns (0,2,3,4,5,6,7,8,9,10) have mixed types. Specify dtype option on import or set low_memory=False.
      df = pd.read_csv(StringIO("\n".join(good)), header=None,
    C:\Users\SEBAS GJ\AppData\Local\Temp\ipykernel_15716\68538246.py:23: UserWarning: Could not infer format, so each element will be parsed individually, falling back to `dateutil`. To ensure parsing is consistent and as-expected, please specify a format.
      df["fecha_hora"] = pd.to_datetime(df["fecha_hora"], errors="coerce")
    C:\Users\SEBAS GJ\AppData\Local\Temp\ipykernel_15716\68538246.py:21: DtypeWarning: Columns (0,2,3) have mixed types. Specify dtype option on import or set low_memory=False.
      df = pd.read_csv(StringIO("\n".join(good)), header=None,
    C:\Users\SEBAS GJ\AppData\Local\Temp\ipykernel_15716\68538246.py:23: UserWarning: Could not infer format, so each element will be parsed individually, falling back to `dateutil`. To ensure parsing is consistent and as-expected, please specify a format.
      df["fecha_hora"] = pd.to_datetime(df["fecha_hora"], errors="coerce")
    C:\Users\SEBAS GJ\AppData\Local\Temp\ipykernel_15716\68538246.py:21: DtypeWarning: Columns (0,2,3) have mixed types. Specify dtype option on import or set low_memory=False.
      df = pd.read_csv(StringIO("\n".join(good)), header=None,
    C:\Users\SEBAS GJ\AppData\Local\Temp\ipykernel_15716\68538246.py:23: UserWarning: Could not infer format, so each element will be parsed individually, falling back to `dateutil`. To ensure parsing is consistent and as-expected, please specify a format.
      df["fecha_hora"] = pd.to_datetime(df["fecha_hora"], errors="coerce")
    C:\Users\SEBAS GJ\AppData\Local\Temp\ipykernel_15716\68538246.py:21: DtypeWarning: Columns (0,2,3,4) have mixed types. Specify dtype option on import or set low_memory=False.
      df = pd.read_csv(StringIO("\n".join(good)), header=None,
    C:\Users\SEBAS GJ\AppData\Local\Temp\ipykernel_15716\68538246.py:23: UserWarning: Could not infer format, so each element will be parsed individually, falling back to `dateutil`. To ensure parsing is consistent and as-expected, please specify a format.
      df["fecha_hora"] = pd.to_datetime(df["fecha_hora"], errors="coerce")
    

    Figuras guardadas: ['fig1_completitud.png', 'fig2_nivel_flags.png', 'fig3_xcorr_lluvia_nivel.png', 'tabla_B4_rolling_origin.csv']
    

# Resumen ejecutivo — Calidad de datos SIATA (4 estaciones, dic-2025 a abr-2026)

## Contexto y método
Se auditaron 869.579 registros minutales de 4 estaciones (pluviométrica 35,
meteorológica 367, piranómetro 6004, nivel 803) con una cadena de control en
cinco etapas (formato/completitud → rango → persistencia/salto → coherencia
interna → coherencia temporal), más métodos estadísticos (residuos STL+MAD) y
de aprendizaje (Isolation Forest), contrastados contra la columna `calidad`
como etiqueta débil. Todo es reproducible: `pytest -q` + notebook + `outputs/`.

## Hallazgos priorizados
| # | Hallazgo | Evidencia | Riesgo | Prioridad |
|---|---|---|---|---|
| 1 | Completitud desigual: 6004 pierde 2.616 min (98,8%); 803 122; 367 43; 35 completa | Fig 1 | Agregados y alertas sesgadas por huecos invisibles | Alta |
| 2 | Radiación nocturna >50 W/m² en 6004 (físicamente imposible) | Regla R_noche + Fig de distribución | Balance de energía y modelos contaminados | Alta |
| 3 | Sensor de nivel 803 congelado (mesetas ≥10 min) y salto de 7,6→20,5 m en 1 min | Fig 2 | Falsos estables / pico espurio en alerta de creciente | Alta |
| 4 | Canales p1/p2 del pluviómetro discrepan 0,254 mm en minutos secos | Regla R_disc | Doble versión del mismo fenómeno sin canal maestro | Media |
| 5 | Relación lluvia→Δnivel con lag óptimo de 1–3 h confirmada | Fig 3 | Oportunidad: predictor adelantado de crecientes | Media (valor) |

## Recomendaciones accionables
1. **30 días:** publicar diccionario de banderas versionado (códigos 151/1502/
   1511/1512/1583 inferidos) y panel de completitud/validez/latencia por estación.
2. **30 días:** mantenimiento al piranómetro 6004 (offset nocturno) y al limnímetro
   803 (congelamientos); revisar telemetría del salto espurio.
3. **60 días:** definir canal maestro p1/p2 o fusión documentada; regla de
   coherencia interna en ingesta.
4. **90 días:** usar el lag 1–3 h lluvia→nivel como señal temprana en el modelo
   de alerta; API pública con agregados horarios y datos abiertos vía CDN.

## Limitaciones
Ventana de 2 h: umbrales no calibrados por temporada; sin red completa para
coherencia espacial; `calidad` tratada como etiqueta débil, no como verdad.


```python

```
