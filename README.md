# Projeto de Pipeline de Dados: NYC Taxi (GCP + dbt)

Este projeto demonstra a construção de um pipeline de dados ELT (Extract, Load, Transform) completo, desde a ingestão de dados brutos até a modelagem analítica final.

Utilizamos os dados de corridas de táxi de NYC (disponíveis no Kaggle) para simular um cenário real. O objetivo é demonstrar competências em ingestão de dados, armazenamento em Data Warehouse na nuvem (BigQuery) e transformação usando ferramentas modernas como o dbt.

## Arquitetura do Pipeline

O fluxo de dados segue as seguintes etapas:

1.  **Extração (Python):** Um script Python (`ingest_raw_data.py`) é responsável por baixar os dados da competição do Kaggle, descompactar o arquivo `.zip` e fazer o upload do `.csv` bruto para um bucket no **Google Cloud Storage (GCS)**.
2.  **Carregamento (Python):** Um segundo script (`load_gcs_to_bigquery.py`) carrega os dados do GCS para uma tabela na camada `raw` (bruta) dentro do **Google BigQuery**.
3.  **Transformação (dbt):** O **dbt** é utilizado para orquestrar todas as transformações SQL dentro do BigQuery, seguindo a **Arquitetura Medalhão**:
    * **Raw (Bruta):** `nyc_taxi_raw.train` (Definida como uma `source` no dbt).
    * **Refined (Prata):** `stg_nyc_taxi_train` (Limpeza, renomeação de colunas, tipagem).
    * **Business (Ouro):** Modelos analíticos (descritos abaixo).



## Tecnologias Utilizadas

* **Cloud:** **Google Cloud Platform (GCP)**
* **Data Warehouse:** **Google BigQuery**
* **Data Lake (Staging):** **Google Cloud Storage (GCS)**
* **Ingestão/Carga:** **Python** (Pandas, `google-cloud-storage`, `google-cloud-bigquery`)
* **Transformação e Modelagem:** **dbt (Data Build Tool)**
* **Versionamento:** **Git**

---

## Modelo de Dados (Star Schema)

Para a camada analítica (`business`), implementamos um modelo **Star Schema** para otimizar e facilitar consultas de BI (Business Intelligence).

O modelo é composto por uma tabela Fato central e duas tabelas de Dimensão:

* **`fct_trips` (Tabela Fato):** Contém as métricas de negócio (como `duracao_viagem_segundos`, `contagem_passageiros`) e as chaves estrangeiras que se conectam às dimensões.
* **`dim_vendor` (Dimensão):** Tabela de dimensão que enriquece o `id_fornecedor` com seu nome legível (ex: 'VeriFone Inc.').
* **`dim_datetime` (Dimensão):** Tabela de dimensão que quebra o `data_hora_embarque` em partes úteis para análise (hora, dia da semana, mês, ano).

Este modelo é então consumido por uma tabela de agregação final: `business_total_viagens_por_fornecedor`.

## Qualidade e Documentação

A integridade do modelo de dados é garantida por **12 testes** configurados no `schema.yml` do dbt. Estes testes validam:
* **Chaves Primárias:** Garantia de que são únicas e não nulas (`unique`, `not_null`).
* **Integridade Referencial:** Garantia de que as chaves estrangeiras na tabela Fato (`fct_trips`) existem nas tabelas de Dimensão (teste `relationships`).

Toda a documentação do projeto, incluindo descrições de colunas e o DAG (Gráfico de Linhagem de Dados), foi gerada usando o comando `dbt docs generate`.

---

## Como Executar o Projeto

### 1. Pré-requisitos

* Python 3.9+
* Conta no GCP com um projeto, bucket GCS e credenciais de Service Account (arquivo `.json`).
* Credenciais do Kaggle (arquivo `kaggle.json`).

### 2. Configuração do Ambiente

1.  Clone este repositório:
    ```bash
    git clone <url-do-repositorio>
    cd <nome-do-repositorio>
    ```
2.  Instale as dependências Python:
    ```bash
    pip install -r requirements.txt
    ```
3.  Configure suas credenciais do GCP (necessário para os scripts de ingestão):
    ```bash
    export GOOGLE_APPLICATION_CREDENTIALS="~/path/para/seu-gcp-credentials.json"
    ```
4.  Configure suas credenciais do Kaggle:
    ```bash
    mkdir -p ~/.kaggle
    echo '{"username":"seu-user","key":"sua-key"}' > ~/.kaggle/kaggle.json
    chmod 600 ~/.kaggle/kaggle.json
    ```

### 3. Executar o Pipeline

1.  **Ingestão (Kaggle -> GCS):**
    (Atualize `GCP_BUCKET_NAME` no script antes de rodar)
    ```bash
    python ingest_raw_data.py
    ```
2.  **Carregamento (GCS -> BigQuery):**
    (Atualize `GCP_BUCKET_NAME` e `BQ_DATASET_ID` no script antes de rodar)
    ```bash
    python load_gcs_to_bigquery.py
    ```
3.  **Configurar o dbt:**
    * Navegue até a pasta do projeto dbt:
        ```bash
        cd meu_projeto_dbt
        ```
    * Configure seu `~/.dbt/profiles.yml` para apontar para o seu `keyfile` do GCP e o `dataset` que você deseja usar para o dbt (ex: `nyc_taxi_analytics`).

4.  **Executar as Transformações (dbt):**
    * **Construir modelos:**
        ```bash
        dbt run
        ```
    * **Validar a qualidade dos dados:**
        ```bash
        dbt test
        ```
    * **Visualizar a documentação:**
        ```bash
        dbt docs generate
        dbt docs serve
        ```
