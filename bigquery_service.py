import os
from typing import Dict
from google.cloud import bigquery
from dotenv import load_dotenv
from google.oauth2 import service_account


load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_ID = os.getenv("DATASET_ID")
TABLE_ID = os.getenv("TABLE_ID")

client = bigquery.Client()

def insert_or_update_patient_data(paciente: Dict):
    """
    Inserta o actualiza un paciente y su prescripción en BigQuery.

    :param paciente: Diccionario con la estructura del paciente.
    """
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    query = f"""
        SELECT * FROM `{table_ref}`
        WHERE paciente_clave = @clave
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("clave", "STRING", paciente["paciente_clave"])
        ]
    )

    result = list(client.query(query, job_config=job_config).result())

    if result:
        # Actualizar paciente: agregar prescripción
        existing = result[0]
        paciente["prescripciones"] += existing["prescripciones"]
        errors = client.insert_rows_json(table_ref, [paciente], row_ids=[existing["paciente_clave"]])
    else:
        # Insertar nuevo paciente
        errors = client.insert_rows_json(table_ref, [paciente])

    if errors:
        raise Exception(f"Error al insertar o actualizar paciente: {errors}")
