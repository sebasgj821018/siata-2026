# Calidad de datos hidrometeorológicos — Prueba técnica SIATA (2026-10-02)
Candidato: Sebastián Gaviria Jaramillo — Senior Data Scientist
Ventana: 2 h (9:00–11:00) + sustentación de 10 min

## Propósito
Detectar, banderar y documentar problemas de calidad en 4 estaciones SIATA
(frecuencia minutal, dic-2025 a abr-2026) con un pipeline reproducible,
probado y listo para evolucionar a producción.

## Estructura
    parte1/            Respuestas conceptuales (markdown)
    parte2/            Notebook de análisis (Tareas A–D) y dashboard/
    src/calidad/       Paquete modular: carga, reglas, IO (E2)
    tests/             Pruebas pytest de las reglas de calidad (E2)
    outputs/           Tablas y figuras versionadas con manifest (E3)
    data/              CSV originales, SOLO LECTURA (no versionados)

## Instalación y ejecución
    python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
    pip install -r requirements.txt
    pytest -q                                  # pruebas de reglas de calidad
    jupyter lab parte2/notebook.ipynb          # análisis completo A–D
    streamlit run parte2/dashboard/app.py      # prototipo (Tarea F)

## Supuestos documentados (no se entregó diccionario)
- `calidad == 1` = válido; demás códigos = banderas (diccionario inferido en C5).
- Frecuencia nominal 1 min; rejilla esperada 151 días × 1.440 = 217.440 min.
- Marcas de tiempo locales (America/Bogotá, UTC-5).
- p1/p2 = canales redundantes del mismo balancín (0,254 mm por volcamiento).

## Herramientas y verificación (transparencia)
Se usó un asistente de IA como pair-programmer para estructura, depuración y
redacción. **Todo resultado cuantitativo fue ejecutado y verificado por el
candidato contra los CSV**: reproducible con `pytest -q` y el notebook.

## Limitaciones conocidas
Ventana de 2 h: sin validación espacial de red completa, sin ajuste fino de
umbrales por temporada y dashboard en modo prototipo.