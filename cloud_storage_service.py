import os
from google.cloud import storage
from uuid import uuid4
from google.oauth2 import service_account



def upload_image_to_bucket(bucket_name: str, image_path: str) -> str:
    """
    Sube una imagen a Cloud Storage y retorna su URL pública.

    :param bucket_name: Nombre del bucket de GCS.
    :param image_path: Ruta local del archivo de imagen.
    :return: URL pública de la imagen subida.
    """
    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    credentials = service_account.Credentials.from_service_account_file(credentials_path)
    client = storage.Client(credentials=credentials)
    
    bucket = client.bucket(bucket_name)
    blob_name = f"prescripciones/{uuid4().hex}_{os.path.basename(image_path)}"
    blob = bucket.blob(blob_name)
    
    blob.upload_from_filename(image_path)


    # Devuelve la ruta interna gs:// para guardar en BigQuery
    return f"gs://{bucket_name}/{blob_name}"
