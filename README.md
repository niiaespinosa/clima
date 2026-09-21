# Clima

## Objetivo
Creaor una pipeline local para analizar datos historicos de clima en la Sierra Norte de Puebla  

## Herramientas

- `niquests` para descargar el dataset
- `DuckDB` como motor analitico local
- archivos `JSON` y `Parquet` para persistencia reproducible
- `uv` para dependencias y ejecucion

Ya no depende de PostgreSQL, variables de entorno, ni tablas creadas a mano.

## Flujo

1. `clima_01bronce.py` descarga el dataset crudo y lo guarda en `data/raw/`.
2. `clima_02plata.py` normaliza la respuesta diaria y la guarda en DuckDB y Parquet.
3. `clima_03Btrans.py` construye la capa oro con features temporales.
4. `clima03_trends.py` genera una grafica anual en `data/plots/`.
5. `clima_04analisis.py` genera un paquete de graficas de validacion en `data/plots/analysis/`.

Todos los artefactos tambien quedan materializados en `data/clima.duckdb`.

## Uso

Instalar dependencias:

```bash
uv sync
```

Ejecutar el flujo completo:

```bash
uv run python clima_pipeline.py run-all
```

Ejecutar por capas:

```bash
uv run python clima_01bronce.py
uv run python clima_02plata.py
uv run python clima_03Btrans.py
uv run python clima03_trends.py
uv run python clima_04analisis.py
```

Opciones utiles:

```bash
uv run python clima_01bronce.py --force
uv run python clima_02plata.py --raw-path data/raw/archivo.json
uv run python clima03_trends.py --show
uv run python clima_04analisis.py --show
```

## Salidas

- `data/raw/*.json`: respuesta original de Open-Meteo
- `data/clima.duckdb`: tablas `clima_bronze`, `clima_plata` y `clima_oro`
- `data/silver/clima_plata.parquet`: capa plata exportada
- `data/gold/clima_oro.parquet`: capa oro exportada
- `data/plots/temperatura_anual.png`: grafica anual
- `data/plots/analysis/*`: graficas exploratorias y reporte de validacion
