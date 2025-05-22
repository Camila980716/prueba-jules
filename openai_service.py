import os
import base64
import requests

from typing import Optional
from dotenv import load_dotenv

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_API_URL = "https://api.openai.com/v1/chat/completions"


def extract_data_from_prescription(image_path: str, prompt: str) -> str:
    """
    Envía una imagen y prompt a la API de OpenAI para extraer información.

    :param image_path: Ruta local de la imagen.
    :param prompt: Prompt específico para extracción de datos.
    :return: Respuesta del modelo como string.
    """
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json"
    }

    with open(image_path, "rb") as image_file:
        image_base64 = base64.b64encode(image_file.read()).decode("utf-8")

    data = {
        "model": "gpt-4.1-mini",
        "messages": [
            {"role": "system", "content": prompt},
            {"role": "user", "content": [
                {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{image_base64}"}}
            ]}
        ],
        "max_tokens": 1500,
        "temperature": 0
    }

    response = requests.post(OPENAI_API_URL, headers=headers, json=data)

    if response.status_code != 200:
        return f"Error en la API de OpenAI: {response.status_code}"

    try:
        content = response.json()["choices"][0]["message"]["content"]
        return content
    except Exception as e:
        return f"Error interpretando la respuesta del modelo: {e}"
