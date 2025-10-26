{{ config(materialized='table') }}

select
    -- Chave Primária
    id_viagem,

    -- Chaves Estrangeiras (Links para as Dimensões)
    id_fornecedor,             -- Link para dim_vendor
    data_hora_embarque,        -- Link para dim_datetime (PK da dim_datetime é data_hora_embarque)

    -- Métricas (Os Fatos)
    contagem_passageiros,
    duracao_viagem_segundos,

    -- Coordenadas (Métricas/Dimensões Degeneradas)
    latitude_embarque,
    longitude_embarque,
    latitude_desembarque,
    longitude_desembarque,

    -- Flag
    flag_armazenado_em_memoria,

    -- Timestamp de Desembarque (mantido para cálculos)
    data_hora_desembarque

from
    {{ ref('stg_nyc_taxi_train') }}