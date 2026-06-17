# dags/energy_data_pipeline.py
"""
Energy Data Pipeline — DAG Apache Airflow

Source : Données de consommation énergétique des bâtiments (data.gouv.fr)
         Dataset DPE (Diagnostic de Performance Énergétique)

Pipeline :
    1. check_csv_available   → vérifie que l'API répond (HTTP HEAD)
    2. extract_energy_data   → télécharge le CSV et le convertit en Parquet
    3. validate_data_quality → DataQualityOperator custom (seuil 95%)
    4. calculate_kpi         → agrégations Pandas (consommation par catégorie)
    5. load_to_warehouse     → INSERT dans PostgreSQL (upsert)
    6. generate_daily_report → résumé XCom
    7. send_notification     → email récapitulatif

Schedule : quotidien à 06h00 UTC
"""

import os
import json
import pendulum
import requests
import pandas as pd
import numpy as np

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from operators.data_quality_operator import DataQualityOperator

# ─────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────

DATASET_URL = (
    "https://www.data.gouv.fr/api/1/datasets/r/"
    "ab58f6c8-2f11-4e20-946a-4c6251546935"
)

RAW_DATA_DIR  = "/opt/airflow/datalake/raw_data"
POSTGRES_CONN = "postgres_data_warehouse"

CRITICAL_COLUMNS = [
    "annee_consommation",
    "meta_categorie_activite",
    "categorie_activite",
    "sous_categorie_activite",
    "nombre_declaration",
    "surface_declaree",
    "consommation_declaree",
]

NUMERIC_COLUMNS = [
    "nombre_declaration",
    "surface_declaree",
    "consommation_declaree",
]


# ─────────────────────────────────────────────────────────────────
# Tâche 1 — Vérifier la disponibilité de la source
# ─────────────────────────────────────────────────────────────────

def _check_csv_available(**kwargs):
    response = requests.head(DATASET_URL, allow_redirects=True, timeout=30)
    if response.status_code != 200:
        raise Exception(
            f"Source indisponible — HTTP {response.status_code} : {DATASET_URL}"
        )


# ─────────────────────────────────────────────────────────────────
# Tâche 2 — Extraction CSV → Parquet
# ─────────────────────────────────────────────────────────────────

def _extract_energy_data(**kwargs):
    """
    Télécharge le CSV depuis data.gouv.fr et le persiste en Parquet.

    Pourquoi Parquet ?
    - Compression columnar (~5x moins volumineux que CSV)
    - Lecture partielle par colonne (Pandas/Spark ne lisent que ce dont ils ont besoin)
    - Types préservés entre exécutions (pas de re-cast à la relecture)
    """
    os.makedirs(RAW_DATA_DIR, exist_ok=True)
    output_path = os.path.join(RAW_DATA_DIR, f"energy_data_{kwargs['ds']}.parquet")

    df = pd.read_csv(DATASET_URL, sep=",", low_memory=False)

    # Normaliser les noms de colonnes (minuscules, sans espaces)
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    df.to_parquet(output_path, index=False, compression="snappy")

    kwargs["ti"].xcom_push(key="output_path",     value=output_path)
    kwargs["ti"].xcom_push(key="rows_extracted",  value=len(df))

    print(f"Extraction terminée : {len(df):,} lignes → {output_path}")


# ─────────────────────────────────────────────────────────────────
# Tâche 4 — Calcul des KPIs
# ─────────────────────────────────────────────────────────────────

def _calculate_kpi(**kwargs):
    """
    Calcule 3 KPIs à partir du Parquet extrait :

    KPI 1 — Consommation totale et moyenne par catégorie d'activité (kWh)
    KPI 2 — Intensité énergétique : kWh/m² par catégorie
             Permet de comparer des bâtiments de tailles différentes.
             Ex : 200 kWh/m² (mauvaise isolation) vs 50 kWh/m² (bâtiment BBC)
    KPI 3 — Résumé global : total déclarations, consommation totale, top secteur
    """
    ti = kwargs["ti"]
    output_path = ti.xcom_pull(task_ids="extract_energy_data", key="output_path")

    df = pd.read_parquet(output_path)

    # Garder uniquement les lignes exploitables
    df = df[
        df["surface_declaree"].notna()
        & df["consommation_declaree"].notna()
        & (df["surface_declaree"] > 0)
        & (df["consommation_declaree"] >= 0)
    ].copy()

    # ── KPI 1 & 2 : Agrégations par catégorie ────────────────────
    kpi = (
        df.groupby("categorie_activite")
        .agg(
            nb_declarations  =("nombre_declaration",    "sum"),
            total_surface_m2 =("surface_declaree",      "sum"),
            total_consumption=("consommation_declaree", "sum"),
            avg_consumption  =("consommation_declaree", "mean"),
        )
        .reset_index()
    )

    # Intensité énergétique (kWh/m²) — indicateur d'efficacité bâtimentaire
    kpi["avg_consumption_m2"] = np.where(
        kpi["total_surface_m2"] > 0,
        kpi["total_consumption"] / kpi["total_surface_m2"],
        0,
    ).round(4)

    kpi = kpi.round(2)

    # ── KPI 3 : Résumé global ─────────────────────────────────────
    top_category = kpi.nlargest(1, "avg_consumption_m2")["categorie_activite"].iloc[0]

    summary = {
        "total_declarations":     int(kpi["nb_declarations"].sum()),
        "total_consumption_kwh":  float(kpi["total_consumption"].sum()),
        "avg_consumption_kwh":    round(float(df["consommation_declaree"].mean()), 2),
        "top_energy_category":    top_category,
        "nb_categories":          len(kpi),
        "rows_processed":         len(df),
    }

    # Log du top 5 pour le monitoring Airflow
    top5 = kpi.nlargest(5, "avg_consumption_m2")[
        ["categorie_activite", "avg_consumption_m2", "nb_declarations"]
    ]
    print("=== Top 5 secteurs les plus énergivores (kWh/m²) ===")
    print(top5.to_string(index=False))
    print(f"\nRésumé : {summary}")

    ti.xcom_push(key="kpi_by_category", value=kpi.to_dict(orient="records"))
    ti.xcom_push(key="kpi_summary",     value=summary)


# ─────────────────────────────────────────────────────────────────
# Tâche 5 — Chargement dans PostgreSQL
# ─────────────────────────────────────────────────────────────────

def _load_to_warehouse(**kwargs):
    """
    Charge les KPIs dans PostgreSQL via upsert (INSERT … ON CONFLICT DO UPDATE).

    Pourquoi upsert ?
    Si le DAG est relancé sur la même date (retry ou backfill Airflow),
    on écrase les valeurs existantes plutôt que de lever une erreur de clé unique.
    """
    ti              = kwargs["ti"]
    date_execution  = kwargs["ds"]
    kpi_by_category = ti.xcom_pull(task_ids="calculate_kpi", key="kpi_by_category")
    summary         = ti.xcom_pull(task_ids="calculate_kpi", key="kpi_summary")

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN)

    # ── Upsert KPIs par catégorie ─────────────────────────────────
    rows = [
        (
            date_execution,
            row["categorie_activite"],
            int(row["nb_declarations"]),
            float(row["total_surface_m2"]),
            float(row["total_consumption"]),
            float(row["avg_consumption"]),
            float(row["avg_consumption_m2"]),
        )
        for row in kpi_by_category
    ]

    hook.insert_rows(
        table="public.energy_kpi_by_category",
        rows=rows,
        replace=True,
        replace_index=["date_execution", "activity_category"],
        target_fields=[
            "date_execution",
            "activity_category",
            "nb_declarations",
            "total_surface_m2",
            "total_consumption",
            "avg_consumption",
            "avg_consumption_m2",
        ],
    )

    # ── Upsert résumé quotidien ───────────────────────────────────
    hook.run(
        """
        INSERT INTO public.energy_kpi_daily_summary
            (date_execution, total_declarations, total_consumption_kwh,
             avg_consumption_kwh, top_category, rows_processed)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (date_execution) DO UPDATE SET
            total_declarations    = EXCLUDED.total_declarations,
            total_consumption_kwh = EXCLUDED.total_consumption_kwh,
            avg_consumption_kwh   = EXCLUDED.avg_consumption_kwh,
            top_category          = EXCLUDED.top_category,
            rows_processed        = EXCLUDED.rows_processed;
        """,
        parameters=(
            date_execution,
            summary["total_declarations"],
            summary["total_consumption_kwh"],
            summary["avg_consumption_kwh"],
            summary["top_energy_category"],
            summary["rows_processed"],
        ),
    )

    print(
        f"Chargement terminé : {len(rows)} catégories insérées pour {date_execution}"
    )


# ─────────────────────────────────────────────────────────────────
# Tâche 6 — Rapport quotidien
# ─────────────────────────────────────────────────────────────────

def _generate_daily_report(**kwargs):
    ti             = kwargs["ti"]
    summary        = ti.xcom_pull(task_ids="calculate_kpi", key="kpi_summary") or {}
    rows_extracted = ti.xcom_pull(task_ids="extract_energy_data", key="rows_extracted") or 0

    report = {
        "pipeline":              "energy_data_pipeline",
        "execution_date":        kwargs["ds"],
        "status":                "SUCCESS",
        "rows_extracted":        rows_extracted,
        "rows_processed":        summary.get("rows_processed", 0),
        "nb_categories":         summary.get("nb_categories", 0),
        "total_consumption_kwh": summary.get("total_consumption_kwh", 0),
        "avg_consumption_kwh":   summary.get("avg_consumption_kwh", 0),
        "top_energy_category":   summary.get("top_energy_category", "N/A"),
    }

    ti.xcom_push(key="daily_report", value=report)
    print(json.dumps(report, indent=2, ensure_ascii=False))
    return report


# ─────────────────────────────────────────────────────────────────
# Définition du DAG
# ─────────────────────────────────────────────────────────────────

with DAG(
    dag_id="energy_data_pipeline",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule_interval="0 6 * * *",
    catchup=False,
    default_args={
        "owner":           "thomas",
        "depends_on_past": False,
        "retries":         3,
        "retry_delay":     pendulum.duration(seconds=30),
    },
    tags=["production", "energy", "etl", "data-quality"],
    doc_md=__doc__,
) as dag:

    check_csv_available = PythonOperator(
        task_id="check_csv_available",
        python_callable=_check_csv_available,
    )

    extract_energy_data = PythonOperator(
        task_id="extract_energy_data",
        python_callable=_extract_energy_data,
    )

    validate_data_quality = DataQualityOperator(
        task_id="validate_data_quality",
        input_path=f"{RAW_DATA_DIR}/energy_data_{{{{ ds }}}}.parquet",
        critical_columns=CRITICAL_COLUMNS,
        numeric_columns=NUMERIC_COLUMNS,
        threshold=0.95,
    )

    calculate_kpi = PythonOperator(
        task_id="calculate_kpi",
        python_callable=_calculate_kpi,
    )

    load_to_warehouse = PythonOperator(
        task_id="load_to_warehouse",
        python_callable=_load_to_warehouse,
    )

    generate_daily_report = PythonOperator(
        task_id="generate_daily_report",
        python_callable=_generate_daily_report,
    )

    send_notification = EmailOperator(
        task_id="send_notification",
        to="thomas84330@gmail.com",
        subject="Rapport Pipeline Énergie — {{ ds }}",
        html_content="""
            <h3>Pipeline Énergie — {{ ds }}</h3>
            <p><b>Statut :</b> Succès</p>
            <pre>{{ ti.xcom_pull(task_ids='generate_daily_report',
                                key='daily_report') | tojson(indent=4) }}</pre>
        """,
    )

    (
        check_csv_available
        >> extract_energy_data
        >> validate_data_quality
        >> calculate_kpi
        >> load_to_warehouse
        >> generate_daily_report
        >> send_notification
    )
