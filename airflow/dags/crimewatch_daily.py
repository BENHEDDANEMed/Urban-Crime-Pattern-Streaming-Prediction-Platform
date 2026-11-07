"""
DAG Airflow : le pipeline batch de CrimeWatch, une fois par jour.

  bronze_to_silver -> check_silver -> weather_history -> silver_to_gold -> check_gold -> train_category

Le streaming (producer + Spark Streaming) tourne en continu à côté et remplit Bronze.
Ce DAG transforme chaque jour Bronze en Silver et Gold, puis réentraîne le modèle.

Chaque tâche lance un script du projet (les mêmes commandes qu'à la main).
Les scripts tournent dans un environnement Python séparé (/opt/jobs-venv) qui contient
PySpark, pandas, XGBoost et MLflow, pour ne pas mélanger leurs versions avec celles d'Airflow.

Si une tâche échoue : 2 nouveaux essais (5 min d'écart), puis la tâche passe en rouge
et les suivantes ne sont pas lancées (on n'entraîne pas un modèle sur des données cassées).
"""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT = os.getenv("CRIMEWATCH_HOME", "/app")   # le projet, monté au MÊME chemin que spark-streaming    # le dossier du projet (monté)
PYTHON = os.getenv("JOBS_PYTHON", "/opt/jobs-venv/bin/python")  # le Python des jobs

# Variables d'environnement ajoutées pour toutes les tâches
# (append_env=True : on garde aussi celles du conteneur, ex. JAVA_HOME, proxy...)
JOB_ENV = {
    "PYTHONPATH": PROJECT,
    "LAKE_PATH": f"{PROJECT}/data/lake",
    "MLFLOW_TRACKING_URI": f"sqlite:///{PROJECT}/mlflow.db",
    "PYSPARK_SUBMIT_ARGS": "--driver-memory 2g pyspark-shell",   # mémoire pour Spark (local)
    # Les "workers" Spark qui exécutent nos fonctions Python (UDF des catégories) doivent
    # utiliser EXACTEMENT le même Python que le script, sinon : "different minor versions".
    "PYSPARK_PYTHON": PYTHON,
    "PYSPARK_DRIVER_PYTHON": PYTHON,
    "MLFLOW_DISABLE_AGENT_HINT": "1",
}


def job(task_id: str, command: str, **kwargs) -> BashOperator:
    return BashOperator(
        task_id=task_id,
        bash_command=f"cd {PROJECT} && {PYTHON} {command}",
        env=JOB_ENV,
        append_env=True,
        **kwargs,
    )


default_args = {
    "owner": "crimewatch",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="crimewatch_daily",
    description="Bronze -> Silver -> météo -> Gold -> modèle (tous les jours à 06:00 UTC)",
    schedule="0 6 * * *",
    start_date=datetime(2026, 11, 1),
    catchup=False,          # ne pas rattraper les jours passés au premier lancement
    max_active_runs=1,      # jamais deux exécutions en même temps (elles écrivent les mêmes fichiers)
    default_args=default_args,
    tags=["crimewatch", "phase2"],
) as dag:

    bronze_to_silver = job("bronze_to_silver", "batch/bronze_to_silver.py")
    check_silver = job("check_silver", "-m batch.quality_checks silver", retries=0)
    weather_history = job("weather_history", "-m batch.weather_history")
    silver_to_gold = job("silver_to_gold", "batch/silver_to_gold.py")
    check_gold = job("check_gold", "-m batch.quality_checks gold", retries=0)
    train_category = job("train_category", "-m ml.train_category",
                         execution_timeout=timedelta(minutes=30))

    bronze_to_silver >> check_silver >> weather_history >> silver_to_gold >> check_gold >> train_category