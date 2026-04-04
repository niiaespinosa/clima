from __future__ import annotations

import argparse
import json
import math
import time
from contextlib import closing
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Sequence

import duckdb
import matplotlib.pyplot as plt
import niquests

PROJECT_ROOT = Path(__file__).resolve().parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"
PLOTS_DIR = DATA_DIR / "plots"
ANALYSIS_DIR = PLOTS_DIR / "analysis"
DATABASE_PATH = DATA_DIR / "clima.duckdb"
ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
MONTH_LABELS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
DOWNLOAD_MAX_ATTEMPTS = 5
DOWNLOAD_BACKOFF_SECONDS = 0.5
RETRY_STATUS_CODES = {500, 502, 503, 504}


@dataclass(frozen=True)
class ArchiveRequest:
    latitude: float = 19.7958
    longitude: float = -97.9355
    location_name: str = "aquixtla"
    start_date: date = date(1990, 1, 1)
    end_date: date | None = None
    timezone: str = "auto"
    daily_metrics: tuple[str, ...] = (
        "temperature_2m_max",
        "temperature_2m_min",
        "temperature_2m_mean",
        "precipitation_sum",
        "rain_sum",
    )

    @property
    def resolved_end_date(self) -> date:
        return self.end_date or (date.today() - timedelta(days=1))

    @property
    def dataset_stem(self) -> str:
        return (
            f"open_meteo_{self.location_name}_"
            f"{self.start_date.isoformat()}_{self.resolved_end_date.isoformat()}"
        )

    def to_query_params(self) -> dict[str, str | float]:
        return {
            "latitude": self.latitude,
            "longitude": self.longitude,
            "start_date": self.start_date.isoformat(),
            "end_date": self.resolved_end_date.isoformat(),
            "daily": ",".join(self.daily_metrics),
            "timezone": self.timezone,
        }


def ensure_directories() -> None:
    for directory in (DATA_DIR, RAW_DIR, SILVER_DIR, GOLD_DIR, PLOTS_DIR, ANALYSIS_DIR):
        directory.mkdir(parents=True, exist_ok=True)


def raw_dataset_path(request: ArchiveRequest) -> Path:
    return RAW_DIR / f"{request.dataset_stem}.json"


def silver_dataset_path() -> Path:
    return SILVER_DIR / "clima_plata.parquet"


def gold_dataset_path() -> Path:
    return GOLD_DIR / "clima_oro.parquet"


def plot_output_path() -> Path:
    return PLOTS_DIR / "temperatura_anual.png"


def analysis_report_path() -> Path:
    return ANALYSIS_DIR / "reporte_validacion.md"


def connect() -> duckdb.DuckDBPyConnection:
    ensure_directories()
    return duckdb.connect(str(DATABASE_PATH))


def _relative(path: Path) -> str:
    return str(path.relative_to(PROJECT_ROOT))


def _unlink_if_exists(path: Path) -> None:
    if path.exists():
        path.unlink()


def _escape_sql_string(value: str) -> str:
    return value.replace("'", "''")


def _latest_raw_dataset() -> Path:
    files = sorted(RAW_DIR.glob("open_meteo_*.json"))
    if not files:
        raise FileNotFoundError(
            "No se encontro un dataset crudo. Ejecuta primero `uv run clima_01bronce.py`."
        )
    return files[-1]


def _complete_year_cutoff(max_fecha: date) -> int | None:
    if max_fecha.month == 12 and max_fecha.day == 31:
        return None
    return max_fecha.year


def _download_archive_payload(request: ArchiveRequest) -> dict:
    last_error: Exception | None = None
    with niquests.Session() as session:
        for attempt in range(1, DOWNLOAD_MAX_ATTEMPTS + 1):
            try:
                response = session.get(
                    ARCHIVE_URL,
                    params=request.to_query_params(),
                    timeout=60,
                )
                if response.status_code in RETRY_STATUS_CODES:
                    raise niquests.HTTPError(
                        f"Open-Meteo devolvio {response.status_code}",
                        request=response.request,
                        response=response,
                    )
                response.raise_for_status()
                return response.json()
            except (niquests.RequestException, niquests.HTTPError) as exc:
                last_error = exc
                if attempt == DOWNLOAD_MAX_ATTEMPTS:
                    break
                wait_seconds = DOWNLOAD_BACKOFF_SECONDS * (2 ** (attempt - 1))
                print(
                    f"Descarga fallida en intento {attempt}/{DOWNLOAD_MAX_ATTEMPTS}: {exc}. "
                    f"Reintentando en {wait_seconds:.1f}s..."
                )
                time.sleep(wait_seconds)

    assert last_error is not None
    raise RuntimeError("No se pudo descargar el dataset tras varios reintentos.") from last_error


def download_raw_dataset(force: bool = False, request: ArchiveRequest | None = None) -> Path:
    request = request or ArchiveRequest()
    ensure_directories()
    target_path = raw_dataset_path(request)

    if target_path.exists() and not force:
        print(f"Dataset local reutilizado: {_relative(target_path)}")
        _materialize_bronze_table(target_path, request)
        return target_path

    print("Descargando dataset historico desde Open-Meteo...")
    payload = _download_archive_payload(request)

    target_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"Dataset guardado en {_relative(target_path)}")

    _materialize_bronze_table(target_path, request)
    return target_path


def _materialize_bronze_table(raw_path: Path, request: ArchiveRequest) -> None:
    raw_payload = raw_path.read_text(encoding="utf-8")
    with closing(connect()) as conn:
        conn.execute(
            """
            CREATE OR REPLACE TABLE clima_bronze AS
            SELECT
                ? AS fuente,
                ? AS dataset_stem,
                CAST(? AS DOUBLE) AS latitude,
                CAST(? AS DOUBLE) AS longitude,
                CAST(? AS DATE) AS start_date,
                CAST(? AS DATE) AS end_date,
                ? AS timezone,
                ? AS raw_file,
                CAST(? AS TIMESTAMP) AS downloaded_at,
                ? AS raw_json
            """,
            [
                ARCHIVE_URL,
                request.dataset_stem,
                request.latitude,
                request.longitude,
                request.start_date.isoformat(),
                request.resolved_end_date.isoformat(),
                request.timezone,
                str(raw_path),
                datetime.now().isoformat(timespec="seconds"),
                raw_payload,
            ],
        )


def _load_raw_payload(raw_path: Path) -> dict:
    return json.loads(raw_path.read_text(encoding="utf-8"))


def build_silver_dataset(raw_path: Path | None = None) -> Path:
    ensure_directories()
    raw_path = raw_path or _latest_raw_dataset()
    payload = _load_raw_payload(raw_path)
    daily = payload["daily"]

    keys = (
        "time",
        "temperature_2m_mean",
        "temperature_2m_min",
        "temperature_2m_max",
        "rain_sum",
        "precipitation_sum",
    )
    lengths = {key: len(daily[key]) for key in keys}
    if len(set(lengths.values())) != 1:
        raise ValueError(f"Las series diarias no tienen la misma longitud: {lengths}")

    rows = [
        (
            fecha,
            float(temperatura_media) if temperatura_media is not None else None,
            float(temperatura_min) if temperatura_min is not None else None,
            float(temperatura_max) if temperatura_max is not None else None,
            float(lluvia) if lluvia is not None else None,
            float(precipitacion) if precipitacion is not None else None,
        )
        for fecha, temperatura_media, temperatura_min, temperatura_max, lluvia, precipitacion in zip(
            daily["time"],
            daily["temperature_2m_mean"],
            daily["temperature_2m_min"],
            daily["temperature_2m_max"],
            daily["rain_sum"],
            daily["precipitation_sum"],
            strict=True,
        )
    ]

    silver_path = silver_dataset_path()
    _unlink_if_exists(silver_path)

    with closing(connect()) as conn:
        conn.execute(
            """
            CREATE OR REPLACE TABLE clima_plata (
                fecha DATE,
                temperatura_media DOUBLE,
                temperatura_min DOUBLE,
                temperatura_max DOUBLE,
                lluvia DOUBLE,
                precipitacion DOUBLE
            )
            """
        )
        conn.executemany(
            """
            INSERT INTO clima_plata VALUES (?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        conn.execute(
            f"""
            COPY clima_plata TO '{_escape_sql_string(str(silver_path))}' (FORMAT PARQUET)
            """
        )

    print(f"Capa plata actualizada: {_relative(silver_path)}")
    return silver_path


def build_gold_dataset() -> Path:
    ensure_directories()
    gold_path = gold_dataset_path()
    _unlink_if_exists(gold_path)

    with closing(connect()) as conn:
        conn.execute("SELECT 1 FROM clima_plata LIMIT 1")
        conn.execute(
            """
            CREATE OR REPLACE TABLE clima_oro AS
            WITH fechas_completas AS (
                SELECT
                    fecha,
                    CASE
                        WHEN temperatura_media BETWEEN -10 AND 50 THEN temperatura_media
                        ELSE NULL
                    END AS temp_hoy,
                    CASE
                        WHEN precipitacion >= 0 THEN precipitacion
                        ELSE NULL
                    END AS lluvia_hoy
                FROM clima_plata
            )
            SELECT
                t1.fecha,
                t1.temp_hoy,
                t1.lluvia_hoy,
                LAG(t1.temp_hoy, 1) OVER (ORDER BY t1.fecha) AS temp_ayer,
                LAG(t1.temp_hoy, 2) OVER (ORDER BY t1.fecha) AS temp_antier,
                AVG(t1.temp_hoy) OVER (
                    ORDER BY t1.fecha
                    ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
                ) AS temp_promedio_7d,
                t2.temp_hoy AS temp_hace_un_anio
            FROM fechas_completas t1
            LEFT JOIN fechas_completas t2
                ON year(t1.fecha) = year(t2.fecha) + 1
                AND month(t1.fecha) = month(t2.fecha)
                AND day(t1.fecha) = day(t2.fecha)
            ORDER BY t1.fecha
            """
        )
        conn.execute(
            f"""
            COPY clima_oro TO '{_escape_sql_string(str(gold_path))}' (FORMAT PARQUET)
            """
        )

    print(f"Capa oro actualizada: {_relative(gold_path)}")
    return gold_path


def plot_temperature_trends(show: bool = False) -> Path:
    ensure_directories()
    output_path = plot_output_path()

    with closing(connect()) as conn:
        max_fecha = conn.execute("SELECT MAX(fecha) FROM clima_plata").fetchone()[0]
        rows = conn.execute(
            """
            SELECT
                year(fecha) AS anio,
                AVG(temperatura_min) AS temperatura_min,
                AVG(temperatura_media) AS temperatura_media,
                AVG(temperatura_max) AS temperatura_max
            FROM clima_plata
            GROUP BY 1
            ORDER BY 1
            """
        ).fetchall()

    if not rows:
        raise ValueError("La capa plata esta vacia. Ejecuta primero `uv run clima_02plata.py`.")

    rows, partial_year = _complete_year_rows(rows, max_fecha)
    years = [row[0] for row in rows]
    min_values = [row[1] for row in rows]
    mean_values = [row[2] for row in rows]
    max_values = [row[3] for row in rows]

    plt.figure(figsize=(12, 6))
    plt.fill_between(
        years,
        min_values,
        max_values,
        color="orange",
        alpha=0.2,
        label="Rango (Min a Max)",
    )
    plt.plot(
        years,
        mean_values,
        color="red",
        linewidth=2,
        marker="o",
        label="Temperatura media",
    )
    title = f"Evolucion historica de la temperatura en Aquixtla ({years[0]} - {years[-1]})"
    if partial_year is not None:
        title += f"\nAno parcial {partial_year} excluido"
    plt.title(title, fontsize=14)
    plt.xlabel("Ano", fontsize=12)
    plt.ylabel("Temperatura promedio (C)", fontsize=12)
    plt.grid(True, linestyle="--", alpha=0.7)
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_path, dpi=160)

    if show:
        plt.show()

    plt.close()
    print(f"Grafica guardada en {_relative(output_path)}")
    return output_path


def run_all(force_download: bool = False, show_plot: bool = False) -> None:
    raw_path = download_raw_dataset(force=force_download)
    build_silver_dataset(raw_path)
    build_gold_dataset()
    plot_temperature_trends(show=show_plot)


def _save_figure(path: Path, show: bool) -> None:
    plt.tight_layout()
    plt.savefig(path, dpi=160)
    if show:
        plt.show()
    plt.close()


def _complete_year_rows(rows: list[tuple], max_fecha: date) -> tuple[list[tuple], int | None]:
    incomplete_year = _complete_year_cutoff(max_fecha)
    if incomplete_year is None:
        return rows, None
    return [row for row in rows if row[0] < incomplete_year], incomplete_year


def _write_report(report: dict[str, object]) -> Path:
    output_path = analysis_report_path()
    lines = [
        "# Reporte de validacion",
        "",
        "## Cobertura",
        "",
        f"- Fecha minima: `{report['min_fecha']}`",
        f"- Fecha maxima: `{report['max_fecha']}`",
        f"- Filas en plata: `{report['silver_rows']}`",
        f"- Filas en oro: `{report['gold_rows']}`",
        f"- Fechas faltantes en plata: `{report['missing_days']}`",
        f"- Fechas duplicadas en plata: `{report['silver_duplicate_dates']}`",
        f"- Fechas duplicadas en oro: `{report['gold_duplicate_dates']}`",
    ]

    if report["partial_year_excluded"] is not None:
        lines.append(
            f"- Ano parcial excluido de comparaciones anuales: `{report['partial_year_excluded']}`"
        )

    lines.extend(
        [
            "",
            "## Rangos y calidad",
            "",
            f"- Temperatura media minima: `{report['temp_media_min']:.2f}` C",
            f"- Temperatura media maxima: `{report['temp_media_max']:.2f}` C",
            f"- Temperatura maxima diaria: `{report['temp_max_max']:.2f}` C",
            f"- Precipitacion maxima diaria: `{report['precip_max']:.2f}` mm",
            f"- Dias con precipitacion cero: `{report['dry_days']}`",
            f"- `temp_hace_un_anio` nulo: `{report['null_temp_hace_un_anio']}` filas",
            "",
            "## Artefactos graficos",
            "",
        ]
    )

    for plot_name in report["plots"]:
        lines.append(f"- `{plot_name}`")

    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return output_path


def run_deep_analysis(show: bool = False) -> Path:
    ensure_directories()
    with closing(connect()) as conn:
        conn.execute("SELECT 1 FROM clima_plata LIMIT 1")
        conn.execute("SELECT 1 FROM clima_oro LIMIT 1")

        min_fecha, max_fecha = conn.execute(
            "SELECT MIN(fecha), MAX(fecha) FROM clima_plata"
        ).fetchone()
        silver_rows = conn.execute("SELECT COUNT(*) FROM clima_plata").fetchone()[0]
        gold_rows = conn.execute("SELECT COUNT(*) FROM clima_oro").fetchone()[0]
        silver_duplicate_dates = conn.execute(
            """
            SELECT COUNT(*)
            FROM (
                SELECT fecha
                FROM clima_plata
                GROUP BY 1
                HAVING COUNT(*) > 1
            )
            """
        ).fetchone()[0]
        gold_duplicate_dates = conn.execute(
            """
            SELECT COUNT(*)
            FROM (
                SELECT fecha
                FROM clima_oro
                GROUP BY 1
                HAVING COUNT(*) > 1
            )
            """
        ).fetchone()[0]
        temp_media_min, temp_media_max, temp_max_max, precip_max, dry_days = conn.execute(
            """
            SELECT
                MIN(temperatura_media),
                MAX(temperatura_media),
                MAX(temperatura_max),
                MAX(precipitacion),
                SUM(CASE WHEN precipitacion = 0 THEN 1 ELSE 0 END)
            FROM clima_plata
            """
        ).fetchone()
        null_temp_hace_un_anio = conn.execute(
            "SELECT COUNT(*) FROM clima_oro WHERE temp_hace_un_anio IS NULL"
        ).fetchone()[0]

        report = {
            "min_fecha": min_fecha,
            "max_fecha": max_fecha,
            "silver_rows": silver_rows,
            "missing_days": (max_fecha - min_fecha).days + 1 - silver_rows,
            "silver_duplicate_dates": silver_duplicate_dates,
            "gold_rows": gold_rows,
            "gold_duplicate_dates": gold_duplicate_dates,
            "temp_media_min": temp_media_min,
            "temp_media_max": temp_media_max,
            "temp_max_max": temp_max_max,
            "precip_max": precip_max,
            "dry_days": dry_days,
            "null_temp_hace_un_anio": null_temp_hace_un_anio,
            "partial_year_excluded": None,
            "plots": [],
        }
        complete_year_cutoff = _complete_year_cutoff(max_fecha)

        annual_temp_rows = conn.execute(
            """
            SELECT
                year(fecha) AS anio,
                AVG(temperatura_min) AS temperatura_min,
                AVG(temperatura_media) AS temperatura_media,
                AVG(temperatura_max) AS temperatura_max
            FROM clima_plata
            GROUP BY 1
            ORDER BY 1
            """
        ).fetchall()

        annual_precip_rows = conn.execute(
            """
            SELECT year(fecha) AS anio, SUM(precipitacion) AS precipitacion_total
            FROM clima_plata
            GROUP BY 1
            ORDER BY 1
            """
        ).fetchall()

        monthly_climatology_rows = conn.execute(
            """
            WITH monthly AS (
                SELECT
                    year(fecha) AS anio,
                    month(fecha) AS mes,
                    AVG(temperatura_media) AS temperatura_media_mensual,
                    SUM(precipitacion) AS precipitacion_mensual
                FROM clima_plata
                WHERE (? IS NULL OR year(fecha) < ?)
                GROUP BY 1, 2
            )
            SELECT
                mes,
                AVG(temperatura_media_mensual) AS temperatura_media,
                AVG(precipitacion_mensual) AS precipitacion_media
            FROM monthly
            GROUP BY 1
            ORDER BY 1
            """,
            [complete_year_cutoff, complete_year_cutoff],
        ).fetchall()

        monthly_temperature_rows = conn.execute(
            """
            SELECT month(fecha) AS mes, temperatura_media
            FROM clima_plata
            ORDER BY 1, 2
            """
        ).fetchall()

        monthly_precipitation_rows = conn.execute(
            """
            SELECT month(fecha) AS mes, precipitacion
            FROM clima_plata
            ORDER BY 1, 2
            """
        ).fetchall()

        anomaly_rows = conn.execute(
            """
            WITH annual AS (
                SELECT year(fecha) AS anio, AVG(temperatura_media) AS temperatura_media
                FROM clima_plata
                WHERE (? IS NULL OR year(fecha) < ?)
                GROUP BY 1
            )
            SELECT
                anio,
                temperatura_media,
                temperatura_media - AVG(temperatura_media) OVER () AS anomalia
            FROM annual
            ORDER BY 1
            """,
            [complete_year_cutoff, complete_year_cutoff],
        ).fetchall()

        extremes_rows = conn.execute(
            """
            WITH thresholds AS (
                SELECT
                    quantile_cont(temperatura_max, 0.9) AS hot_threshold,
                    quantile_cont(precipitacion, 0.95) AS wet_threshold
                FROM clima_plata
            )
            SELECT
                year(fecha) AS anio,
                SUM(CASE WHEN temperatura_max >= hot_threshold THEN 1 ELSE 0 END) AS dias_calidos_extremos,
                SUM(CASE WHEN precipitacion >= wet_threshold THEN 1 ELSE 0 END) AS dias_lluviosos_extremos
            FROM clima_plata, thresholds
            GROUP BY 1
            ORDER BY 1
            """
        ).fetchall()

        heatmap_rows = conn.execute(
            """
            SELECT
                year(fecha) AS anio,
                month(fecha) AS mes,
                AVG(temperatura_media) AS temperatura_media
            FROM clima_plata
            GROUP BY 1, 2
            ORDER BY 1, 2
            """
        ).fetchall()

        feature_rows = conn.execute(
            """
            SELECT temp_hoy, temp_promedio_7d, temp_hace_un_anio
            FROM clima_oro
            WHERE temp_hoy IS NOT NULL
              AND temp_promedio_7d IS NOT NULL
              AND temp_hace_un_anio IS NOT NULL
            """
        ).fetchall()

    annual_temp_rows, partial_year = _complete_year_rows(annual_temp_rows, report["max_fecha"])
    annual_precip_rows, _ = _complete_year_rows(annual_precip_rows, report["max_fecha"])
    anomaly_rows, _ = _complete_year_rows(anomaly_rows, report["max_fecha"])
    extremes_rows, _ = _complete_year_rows(extremes_rows, report["max_fecha"])
    report["partial_year_excluded"] = partial_year

    years = [row[0] for row in annual_temp_rows]
    min_values = [row[1] for row in annual_temp_rows]
    mean_values = [row[2] for row in annual_temp_rows]
    max_values = [row[3] for row in annual_temp_rows]

    plt.figure(figsize=(12, 6))
    plt.fill_between(years, min_values, max_values, color="orange", alpha=0.2, label="Rango anual")
    plt.plot(years, mean_values, color="red", linewidth=2, marker="o", label="Temperatura media")
    title = "Temperatura media anual con rango min-max"
    if partial_year is not None:
        title += f" (anos completos, {partial_year} excluido)"
    plt.title(title)
    plt.xlabel("Ano")
    plt.ylabel("Temperatura promedio (C)")
    plt.grid(True, linestyle="--", alpha=0.7)
    plt.legend()
    annual_temp_path = ANALYSIS_DIR / "01_temperatura_anual.png"
    _save_figure(annual_temp_path, show)
    report["plots"].append(_relative(annual_temp_path))

    plt.figure(figsize=(12, 6))
    precip_years = [row[0] for row in annual_precip_rows]
    precip_totals = [row[1] for row in annual_precip_rows]
    plt.bar(precip_years, precip_totals, color="#1f77b4", alpha=0.85)
    title = "Precipitacion anual acumulada"
    if partial_year is not None:
        title += f" (anos completos, {partial_year} excluido)"
    plt.title(title)
    plt.xlabel("Ano")
    plt.ylabel("Precipitacion total (mm)")
    plt.grid(True, axis="y", linestyle="--", alpha=0.5)
    annual_precip_path = ANALYSIS_DIR / "02_precipitacion_anual.png"
    _save_figure(annual_precip_path, show)
    report["plots"].append(_relative(annual_precip_path))

    months = [row[0] for row in monthly_climatology_rows]
    monthly_temps = [row[1] for row in monthly_climatology_rows]
    monthly_precip = [row[2] for row in monthly_climatology_rows]
    fig, axes = plt.subplots(2, 1, figsize=(12, 9), sharex=True)
    axes[0].plot(months, monthly_temps, color="firebrick", marker="o")
    axes[0].set_title("Climatologia mensual promedio")
    axes[0].set_ylabel("Temperatura media (C)")
    axes[0].grid(True, linestyle="--", alpha=0.5)
    axes[1].bar(months, monthly_precip, color="teal", alpha=0.85)
    axes[1].set_ylabel("Precipitacion media mensual (mm)")
    axes[1].set_xlabel("Mes")
    axes[1].grid(True, axis="y", linestyle="--", alpha=0.5)
    axes[1].set_xticks(months, MONTH_LABELS)
    monthly_climatology_path = ANALYSIS_DIR / "03_climatologia_mensual.png"
    _save_figure(monthly_climatology_path, show)
    report["plots"].append(_relative(monthly_climatology_path))

    temp_groups = [[] for _ in range(12)]
    for month, value in monthly_temperature_rows:
        temp_groups[month - 1].append(value)
    plt.figure(figsize=(13, 6))
    plt.boxplot(temp_groups, tick_labels=MONTH_LABELS, showfliers=False)
    plt.title("Distribucion mensual de temperatura media diaria")
    plt.xlabel("Mes")
    plt.ylabel("Temperatura media diaria (C)")
    plt.grid(True, axis="y", linestyle="--", alpha=0.4)
    temperature_boxplot_path = ANALYSIS_DIR / "04_boxplot_temperatura_mensual.png"
    _save_figure(temperature_boxplot_path, show)
    report["plots"].append(_relative(temperature_boxplot_path))

    precipitation_groups = [[] for _ in range(12)]
    for month, value in monthly_precipitation_rows:
        precipitation_groups[month - 1].append(value)
    plt.figure(figsize=(13, 6))
    plt.boxplot(precipitation_groups, tick_labels=MONTH_LABELS, showfliers=False)
    plt.title("Distribucion mensual de precipitacion diaria")
    plt.xlabel("Mes")
    plt.ylabel("Precipitacion diaria (mm)")
    plt.grid(True, axis="y", linestyle="--", alpha=0.4)
    precipitation_boxplot_path = ANALYSIS_DIR / "05_boxplot_precipitacion_mensual.png"
    _save_figure(precipitation_boxplot_path, show)
    report["plots"].append(_relative(precipitation_boxplot_path))

    anomaly_years = [row[0] for row in anomaly_rows]
    anomalies = [row[2] for row in anomaly_rows]
    anomaly_colors = ["#b22222" if value >= 0 else "#1f4e79" for value in anomalies]
    plt.figure(figsize=(12, 6))
    plt.bar(anomaly_years, anomalies, color=anomaly_colors)
    plt.axhline(0, color="black", linewidth=1)
    title = "Anomalia anual de temperatura media"
    if partial_year is not None:
        title += f" ({partial_year} excluido)"
    plt.title(title)
    plt.xlabel("Ano")
    plt.ylabel("Desvio respecto al promedio historico (C)")
    plt.grid(True, axis="y", linestyle="--", alpha=0.4)
    anomaly_path = ANALYSIS_DIR / "06_anomalias_temperatura.png"
    _save_figure(anomaly_path, show)
    report["plots"].append(_relative(anomaly_path))

    plt.figure(figsize=(12, 6))
    extreme_years = [row[0] for row in extremes_rows]
    hot_days = [row[1] for row in extremes_rows]
    wet_days = [row[2] for row in extremes_rows]
    plt.plot(extreme_years, hot_days, label="Dias calidos extremos", color="orangered", linewidth=2)
    plt.plot(extreme_years, wet_days, label="Dias lluviosos extremos", color="royalblue", linewidth=2)
    title = "Conteo anual de extremos climaticos"
    if partial_year is not None:
        title += f" ({partial_year} excluido)"
    plt.title(title)
    plt.xlabel("Ano")
    plt.ylabel("Dias por ano")
    plt.grid(True, linestyle="--", alpha=0.5)
    plt.legend()
    extremes_path = ANALYSIS_DIR / "07_extremos_anuales.png"
    _save_figure(extremes_path, show)
    report["plots"].append(_relative(extremes_path))

    heatmap_matrix: dict[int, list[float]] = {}
    for year, month, value in heatmap_rows:
        heatmap_matrix.setdefault(year, [math.nan] * 12)[month - 1] = value
    heatmap_years = sorted(heatmap_matrix)
    matrix = [heatmap_matrix[year] for year in heatmap_years]
    plt.figure(figsize=(12, 9))
    plt.imshow(matrix, aspect="auto", cmap="coolwarm")
    plt.colorbar(label="Temperatura media mensual (C)")
    plt.title("Heatmap de temperatura media mensual")
    plt.xlabel("Mes")
    plt.ylabel("Ano")
    plt.xticks(range(12), MONTH_LABELS)
    y_ticks = list(range(0, len(heatmap_years), max(1, len(heatmap_years) // 8)))
    plt.yticks(y_ticks, [heatmap_years[index] for index in y_ticks])
    heatmap_path = ANALYSIS_DIR / "08_heatmap_temperatura_mensual.png"
    _save_figure(heatmap_path, show)
    report["plots"].append(_relative(heatmap_path))

    sampled_feature_rows = feature_rows[::7]
    plt.figure(figsize=(12, 5))
    plt.subplot(1, 2, 1)
    plt.scatter(
        [row[1] for row in sampled_feature_rows],
        [row[0] for row in sampled_feature_rows],
        s=10,
        alpha=0.18,
        color="darkred",
    )
    plt.xlabel("Promedio 7 dias previos (C)")
    plt.ylabel("Temperatura actual (C)")
    plt.title("Validacion: temp_hoy vs temp_promedio_7d")
    plt.grid(True, linestyle="--", alpha=0.3)

    plt.subplot(1, 2, 2)
    plt.scatter(
        [row[2] for row in sampled_feature_rows],
        [row[0] for row in sampled_feature_rows],
        s=10,
        alpha=0.18,
        color="darkblue",
    )
    plt.xlabel("Temperatura hace un ano (C)")
    plt.ylabel("Temperatura actual (C)")
    plt.title("Validacion: temp_hoy vs temp_hace_un_anio")
    plt.grid(True, linestyle="--", alpha=0.3)
    feature_validation_path = ANALYSIS_DIR / "09_validacion_features.png"
    _save_figure(feature_validation_path, show)
    report["plots"].append(_relative(feature_validation_path))

    report_path = _write_report(report)
    print(f"Analisis y validacion guardados en {_relative(ANALYSIS_DIR)}")
    print(f"Reporte disponible en {_relative(report_path)}")
    return report_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Pipeline local de clima con dataset descargado, DuckDB y Parquet."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    download_parser = subparsers.add_parser("download", help="Descarga el dataset y actualiza bronze.")
    download_parser.add_argument("--force", action="store_true", help="Vuelve a descargar el dataset.")

    silver_parser = subparsers.add_parser("silver", help="Construye la capa plata desde el JSON local.")
    silver_parser.add_argument(
        "--raw-path",
        type=Path,
        help="Ruta opcional a un dataset JSON ya descargado.",
    )

    subparsers.add_parser("gold", help="Construye la capa oro desde DuckDB.")

    plot_parser = subparsers.add_parser("plot", help="Genera una grafica anual desde la capa plata.")
    plot_parser.add_argument("--show", action="store_true", help="Muestra la grafica ademas de guardarla.")

    run_all_parser = subparsers.add_parser("run-all", help="Ejecuta download, silver, gold y plot.")
    run_all_parser.add_argument("--force", action="store_true", help="Vuelve a descargar el dataset.")
    run_all_parser.add_argument("--show", action="store_true", help="Muestra la grafica al final.")

    analysis_parser = subparsers.add_parser("analyze", help="Genera un paquete amplio de graficas y validaciones.")
    analysis_parser.add_argument("--show", action="store_true", help="Muestra cada grafica ademas de guardarla.")

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "download":
        download_raw_dataset(force=args.force)
        return 0

    if args.command == "silver":
        build_silver_dataset(raw_path=args.raw_path)
        return 0

    if args.command == "gold":
        build_gold_dataset()
        return 0

    if args.command == "plot":
        plot_temperature_trends(show=args.show)
        return 0

    if args.command == "run-all":
        run_all(force_download=args.force, show_plot=args.show)
        return 0

    if args.command == "analyze":
        run_deep_analysis(show=args.show)
        return 0

    parser.error(f"Comando no soportado: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
