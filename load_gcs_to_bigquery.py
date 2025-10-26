from google.cloud import bigquery
import logging

# Configura o logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s:root:%(message)s')

# --- Configuração ---
# O SDK usará automaticamente as credenciais da variável de ambiente
# GOOGLE_APPLICATION_CREDENTIALS que já configuramos.

# 1. Definições do GCS (Fonte)
GCP_BUCKET_NAME = 'meu-bucket-nyc-taxi-2025'
GCS_BLOB_NAME = 'raw/nyc_taxi_train.csv'
GCS_URI = f"gs://{GCP_BUCKET_NAME}/{GCS_BLOB_NAME}"

# 2. Definições do BigQuery (Destino)
# O ID do Projeto é pego automaticamente das credenciais.
BQ_DATASET_ID = 'nyc_taxi_raw'
BQ_TABLE_ID = 'train' # Nome da nossa nova tabela na camada raw

def load_csv_from_gcs_to_bigquery():
    """
    Carrega um arquivo CSV do Google Cloud Storage para uma tabela no BigQuery.
    """
    logging.info(f"Iniciando job de carregamento do GCS ({GCS_URI}) para o BigQuery ({BQ_DATASET_ID}.{BQ_TABLE_ID})...")

    # 1. Inicializa o cliente do BigQuery
    client = bigquery.Client()

    # 2. Define o ID completo da tabela (projeto.dataset.tabela)
    # O client.project pega o ID do projeto automaticamente do arquivo de credenciais
    table_id = f"{client.project}.{BQ_DATASET_ID}.{BQ_TABLE_ID}"

    # 3. Configura o Job de Carregamento
    job_config = bigquery.LoadJobConfig(
        # Especifica que o BigQuery deve detectar o schema automaticamente
        autodetect=True, 
        
        # Informa que o arquivo fonte é um CSV
        source_format=bigquery.SourceFormat.CSV,
        
        # Informa que o CSV tem uma linha de cabeçalho (header)
        skip_leading_rows=1, 
        
        # Define a política de escrita: se a tabela existir, ela será substituída.
        # Ótimo para re-executar pipelines.
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE, 
    )

    # 4. Inicia o Job
    try:
        load_job = client.load_table_from_uri(
            GCS_URI,          # O caminho 'gs://...' do arquivo
            table_id,         # O ID 'projeto.dataset.tabela' de destino
            job_config=job_config
        )

        logging.info(f"Job de carregamento iniciado. ID do Job: {load_job.job_id}")

        # Espera o job ser concluído
        load_job.result()  

        # 5. Verifica o resultado
        destination_table = client.get_table(table_id)
        logging.info(f"Carregamento concluído! Tabela '{table_id}' criada com {destination_table.num_rows} linhas.")

    except Exception as e:
        logging.error(f"Falha no job de carregamento do BigQuery: {e}")
        raise

def main():
    try:
        load_csv_from_gcs_to_bigquery()
        logging.info("Carregamento GCS -> BigQuery concluído com sucesso!")
    except Exception as e:
        logging.error(f"Falha no pipeline de carregamento para o BigQuery.")

if __name__ == "__main__":
    main()