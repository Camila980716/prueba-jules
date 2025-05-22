import os
import json
import logging
from typing import Union
from uuid import uuid4

from openai_service import extract_data_from_prescription
from cloud_storage_service import upload_image_to_bucket
from bigquery_service import insert_or_update_patient_data


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PIPProcessor:
    """
    Procesador de Imágenes y Prescripciones (PIP)
    """

    def __init__(self):
        self.bucket_name = os.getenv("BUCKET_PRESCRIPCIONES")
        self.prompt_path = os.getenv("PROMPT_PIP_PATH", "prompt_PIP.txt")

    def process_image(self, image_path: str, session_id: str) -> Union[str, dict]:
        """
        Procesa la imagen, extrae datos con LLM y guarda en GCS + BigQuery.

        :param image_path: Ruta local del archivo de imagen.
        :param session_id: ID de sesión actual.
        :return: Mensaje de error o datos extraídos.
        """
        # Paso 1: Leer el prompt
        with open(self.prompt_path, "r", encoding="utf-8") as f:
            prompt = f.read()

        # Paso 2: Llamar al servicio OpenAI con imagen y prompt
        response = extract_data_from_prescription(image_path, prompt)

        if isinstance(response, str) and "fórmula médica válida" in response:
            return response

        try:
            data = json.loads(response).get("datos", {})
        except Exception as e:
            logger.error(f"Respuesta inválida del modelo: {e}")
            return "Hubo un error procesando la fórmula médica."

        if not data.get("tipo_documento") or not data.get("numero_documento"):
            return "No se pudieron extraer datos suficientes para identificar al paciente."

        # Paso 3: Subir imagen a Cloud Storage
        try:
            image_url = upload_image_to_bucket(self.bucket_name, image_path)
        except Exception as e:
            logger.error(f"Error subiendo imagen a Storage: {e}")
            return "Error al subir la fórmula al sistema."

        # Paso 4: Preparar estructura para BigQuery
        paciente_clave = f"CO{data['tipo_documento']}{data['numero_documento']}"

        prescripcion = {
            "id_session": session_id,
            "url_prescripcion": image_url,
            "categoria_riesgo": None,
            "diagnostico": data.get("diagnostico"),
            "IPS": data.get("ips"),
            "medicamentos": data.get("medicamentos", [])
        }

        paciente_record = {
            "paciente_clave": paciente_clave,
            "pais": "CO",
            "tipo_documento": data.get("tipo_documento"),
            "numero_documento": data.get("numero_documento"),
            "nombre_paciente": data.get("paciente"),
            "telefono_contacto": data.get("telefono", []),
            "regimen": data.get("regimen"),
            "ciudad": data.get("ciudad"),
            "direccion": data.get("direccion"),
            "eps_cruda": data.get("eps"),
            "prescripciones": [prescripcion]
        }

        try:
            insert_or_update_patient_data(paciente_record)
        except Exception as e:
            logger.error(f"Error al insertar en BigQuery: {e}")
            return "Error al registrar los datos en el sistema."

        return data
