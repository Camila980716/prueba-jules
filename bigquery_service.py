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
    """
    table_ref_str = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    # Dynamically build query components
    update_set_clauses = []
    insert_columns = []
    insert_values_placeholders = []
    query_params = []

    # Define schema for prescriptions struct (used for query parameter)
    # This should match the BigQuery table schema for the prescripciones column
    prescription_struct_type = bigquery.StructType(
        [
            bigquery.SchemaField("medicamento", "STRING"),
            bigquery.SchemaField("dosis", "STRING"),
            bigquery.SchemaField("fecha", "STRING"), 
            # Assuming DATE stored as STRING, adjust if actual BQ type is DATE
        ]
    )

    # Ensure paciente_clave is the first parameter for the USING clause
    if "paciente_clave" not in paciente:
        raise ValueError("paciente_clave is missing from input and is required for MERGE.")
    
    # Add paciente_clave parameter first (important for USING clause if referenced by name like @paciente_clave)
    # Although the USING clause here uses a fixed name K_paciente_clave for clarity.
    query_params.append(bigquery.ScalarQueryParameter("paciente_clave", "STRING", paciente["paciente_clave"]))

    for key, value in paciente.items():
        # Add all keys to insert columns and value placeholders
        insert_columns.append(key)
        insert_values_placeholders.append(f"@{key}") # Use @key for all value placeholders

        if key == "paciente_clave":
            # Already added to query_params, not added to update_set_clauses
            pass
        elif key == "prescripciones":
            # For UPDATE: ARRAY_CONCAT existing with new, then select distinct
            update_set_clauses.append(f"prescripciones = ARRAY(SELECT DISTINCT x FROM UNNEST(ARRAY_CONCAT(target_table.prescripciones, @{key})) x)")
            
            prescriptions_data = []
            if isinstance(value, list): # Ensure value is a list
                prescriptions_data = [
                    # Ensure order matches StructType definition
                    (p.get("medicamento"), p.get("dosis"), p.get("fecha")) for p in value
                ]
            
            query_params.append(
                bigquery.ArrayQueryParameter(
                    key, # param name "prescripciones"
                    prescription_struct_type, # param type for array elements (StructType object)
                    prescriptions_data # data for the array (list of tuples)
                )
            )
        else:
            # For other fields, add to UPDATE SET clause and create ScalarQueryParameter
            param_type = "STRING"  # Default to STRING
            if isinstance(value, bool):
                param_type = "BOOL"
            elif isinstance(value, int):
                param_type = "INT64"
            elif isinstance(value, float):
                param_type = "FLOAT64"
            # Add other type checks if necessary (e.g., for DATE, TIMESTAMP)
            # For example, if 'fecha' fields in patient root were DATE:
            # elif isinstance(value, datetime.date):
            #     param_type = "DATE"
            
            update_set_clauses.append(f"{key} = @{key}")
            # Add to query_params if not already added (e.g. if patient_clave was not handled first)
            # However, with current loop structure, all non-clave, non-prescripcion items will be added here.
            if not any(p.name == key for p in query_params): # Avoid re-adding paciente_clave if loop order changes
                 query_params.append(bigquery.ScalarQueryParameter(key, param_type, value))


    # Construct the MERGE query
    # Using a fixed alias K_paciente_clave for the source key to avoid issues if a field is also named 'K'
    merge_query = f"""
    MERGE `{table_ref_str}` AS target_table
    USING (SELECT @paciente_clave AS K_paciente_clave) AS source_table
    ON target_table.paciente_clave = source_table.K_paciente_clave
    WHEN MATCHED THEN
        UPDATE SET {', '.join(update_set_clauses)}
    WHEN NOT MATCHED BY TARGET THEN
        INSERT ({', '.join(insert_columns)})
        VALUES ({', '.join(insert_values_placeholders)})
    """

    job_config = bigquery.QueryJobConfig(query_parameters=query_params)

    try:
        # Execute the query and wait for completion
        client.query(merge_query, job_config=job_config).result()
    except Exception as e:
        # For debugging, it can be helpful to print the generated query and parameters
        # detailed_params = [(p.name, p.value, p.parameter_type.type if hasattr(p.parameter_type, 'type') else p.parameter_type) for p in query_params]
        # print(f"Generated MERGE query:\n{merge_query}")
        # print(f"Query parameters:\n{detailed_params}")
        raise Exception(f"Error during MERGE operation: {e}")
