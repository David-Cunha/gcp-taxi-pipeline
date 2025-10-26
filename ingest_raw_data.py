import os
import zipfile
from kaggle.api.kaggle_api_extended import KaggleApi
from google.cloud import storage
import logging

# Configura o logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s:root:%(message)s')

# --- Configuração ---
KAGGLE_COMPETITION = 'nyc-taxi-trip-duration'
KAGGLE_FILE = 'train.zip'      # <-- CORREÇÃO 1: O arquivo a ser baixado é .zip
LOCAL_ZIP_PATH = 'train.zip'   # <-- CORREÇÃO 2: O caminho local do zip
LOCAL_CSV_PATH = 'train.csv'   # <-- O arquivo que será extraído

# ATUALIZE ESTA LINHA (A sua já está correta)
GCP_BUCKET_NAME = 'meu-bucket-nyc-taxi-2025' 
GCS_BLOB_NAME = 'raw/nyc_taxi_train.csv' # Caminho dentro do bucket

def download_data_from_kaggle():
    """Autentica e baixa os dados do Kaggle."""
    logging.info("Iniciando download do Kaggle...")
    api = KaggleApi()
    api.authenticate()

    api.competition_download_file(
        KAGGLE_COMPETITION,
        KAGGLE_FILE, # Pede o 'train.zip'
        path='.',
        force=True 
    )
    logging.info(f"Arquivo '{LOCAL_ZIP_PATH}' baixado.")

def unzip_file():
    """Descompacta o arquivo baixado."""
    logging.info(f"Descompactando '{LOCAL_ZIP_PATH}'...")
    with zipfile.ZipFile(LOCAL_ZIP_PATH, 'r') as zip_ref:
        # Extrai o arquivo .csv (que presumimos ter o nome 'train.csv')
        zip_ref.extract(LOCAL_CSV_PATH) 
    logging.info(f"Arquivo '{LOCAL_CSV_PATH}' extraído.")

def upload_to_gcs():
    """Faz o upload do arquivo CSV para o Google Cloud Storage."""
    logging.info(f"Iniciando upload para GCS (Bucket: {GCP_BUCKET_NAME})...")

    storage_client = storage.Client()
    bucket = storage_client.bucket(GCP_BUCKET_NAME)
    blob = bucket.blob(GCS_BLOB_NAME)

    # Faz o upload do arquivo .csv extraído
    blob.upload_from_filename(LOCAL_CSV_PATH) 
    
    logging.info(f"Upload concluído! Arquivo disponível em: gs://{GCP_BUCKET_NAME}/{GCS_BLOB_NAME}")

def cleanup_local_files():
    """Remove os arquivos locais para limpar o ambiente."""
    logging.info("Limpando arquivos locais...")
    
    if os.path.exists(LOCAL_ZIP_PATH):
        os.remove(LOCAL_ZIP_PATH)
        logging.info(f"Arquivo local '{LOCAL_ZIP_PATH}' removido.")
        
    if os.path.exists(LOCAL_CSV_PATH):
        os.remove(LOCAL_CSV_PATH)
        logging.info(f"Arquivo local '{LOCAL_CSV_PATH}' removido.")

def main():
    try:
        download_data_from_kaggle()
        unzip_file() # <-- CORREÇÃO 3: A etapa de unzip é necessária
        upload_to_gcs()
        
    except Exception as e:
        logging.error(f"Ocorreu um erro no pipeline: {e}")
    finally:
        # Limpa os arquivos independentemente de sucesso ou falha
        cleanup_local_files()
        logging.info("Execução do pipeline finalizada.")

if __name__ == "__main__":
    main()