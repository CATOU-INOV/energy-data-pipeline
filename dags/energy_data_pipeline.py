# dags/energy_data_pipeline.py
import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import requests
import pandas as pd
import os
from operators.data_quality_operator import DataQualityOperator  # <-- Assurez-vous que le chemin est correct


# --- Fonctions Python pour certaines tâches ---
def _check_csv_available(**kwargs):
    url = "https://www.data.gouv.fr/api/1/datasets/r/ab58f6c8-2f11-4e20-946a-4c6251546935"
    try:
        response = requests.head(url, allow_redirects=True)
        if response.status_code != 200:
            raise Exception(f"CSV non disponible, status code : {response.status_code}")
    except Exception as e:
        raise Exception(f"Erreur de vérification : {e}")


def _extract_energy_data(**kwargs):
    url = "https://www.data.gouv.fr/api/1/datasets/r/ab58f6c8-2f11-4e20-946a-4c6251546935"
    output_dir = "/opt/airflow/datalake/raw_data"
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"energy_data_{kwargs['ds']}.parquet")

    try:
        df = pd.read_csv(url, sep=',', low_memory=False)
        df.to_parquet(output_path, index=False)
        print(f"Données brutes stockées dans : {output_path}")
    except Exception as e:
        raise Exception(f"Erreur lors de l'extraction ou conversion : {e}")


def _calculate_kpi(**kwargs):
    # Exemple de calcul des KPI
    print("Calcul des KPI : Consommation moyenne, évolution mensuelle, classement énergivore...")
    return True


def _generate_daily_report(**kwargs):
    report = {
        "pipeline_name": "energy_data_pipeline",
        "execution_date": kwargs['ds'],
        "status": "SUCCESS",
        "rows_processed": "50000",
        "task_execution_times": {
            "extract_energy_data": "10s",
            "validate_data_quality": "5s",
            "calculate_kpi": "15s"
        },
        "alerts": []
    }
    kwargs['ti'].xcom_push(key='daily_report', value=report)
    print("Rapport généré et stocké dans XCom.")
    return report


# --- Définition du DAG ---
with DAG(
    dag_id="energy_data_pipeline",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule_interval="0 6 * * *",
    catchup=False,
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "retries": 3,
        "retry_delay": pendulum.duration(seconds=30),
    },
    tags=["production", "energy", "etl"],
) as dag:

    # 1. Vérifier la disponibilité du CSV
    check_csv_available = PythonOperator(
        task_id="check_csv_available",
        python_callable=_check_csv_available
    )

    # 2. Télécharger et stocker le CSV en Parquet
    extract_energy_data = PythonOperator(
        task_id="extract_energy_data",
        python_callable=_extract_energy_data
    )

    # 3. Vérifier la qualité des données (DataQualityOperator)
    validate_data_quality = DataQualityOperator(
        task_id="validate_data_quality",
        input_path="/opt/airflow/datalake/raw_data/energy_data_{{ ds }}.parquet",
        critical_columns=[
            "annee_consommation",
            "meta_categorie_activite",
            "categorie_activite",
            "sous_categorie_activite",
            "nombre_declaration",
            "surface_declaree",
            "consommation_declaree"
        ],
        numeric_columns=[
            "nombre_declaration",
            "surface_declaree",
            "consommation_declaree"
        ],
        threshold=0.95
    )

    # 4. Calcul des KPI
    calculate_kpi = PythonOperator(
        task_id="calculate_kpi",
        python_callable=_calculate_kpi
    )

    # 5. Charger les KPI dans Postgres
    load_to_warehouse = PostgresOperator(
        task_id="load_to_warehouse",
        postgres_conn_id="postgres_data_warehouse",
        sql=[
            """
            CREATE TABLE IF NOT EXISTS public.energy_data_kpi (
                date_execution DATE,
                activity_category VARCHAR(255),
                average_consumption NUMERIC,
                kpi_score INT
            );
            """,
            """
            INSERT INTO public.energy_data_kpi (date_execution, activity_category, average_consumption, kpi_score)
            VALUES ('{{ ds }}', 'Bureaux', 150.5, 85)
            ON CONFLICT (date_execution, activity_category) DO UPDATE
            SET average_consumption = EXCLUDED.average_consumption,
                kpi_score = EXCLUDED.kpi_score;
            """
        ]
    )

    # 6. Générer le rapport quotidien
    generate_daily_report = PythonOperator(
        task_id="generate_daily_report",
        python_callable=_generate_daily_report
    )

    # 7. Envoyer la notification par email
    send_notification = EmailOperator(
        task_id="send_notification",
        to="votre.email@exemple.com",
        subject="Rapport Quotidien Pipeline Énergie - {{ ds }}",
        html_content="""
            <h3>Rapport Quotidien Pipeline Énergie - {{ ds }}</h3>
            <p>Le pipeline <b>energy_data_pipeline</b> a terminé son exécution.</p>
            <p><b>Statut :</b> Succès</p>
            <pre>{{ ti.xcom_pull(task_ids='generate_daily_report', key='daily_report') | tojson(indent=4) }}</pre>
        """
    )

# --- Définition des dépendances ---
check_csv_available >> extract_energy_data
extract_energy_data >> validate_data_quality
validate_data_quality >> calculate_kpi
calculate_kpi >> load_to_warehouse
load_to_warehouse >> generate_daily_report
generate_daily_report >> send_notification
