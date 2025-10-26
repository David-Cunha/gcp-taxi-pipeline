{{ config(materialized='view') }}

select
    -- IDs e Chaves
    id as id_viagem,
    vendor_id as id_fornecedor,
    
    -- Timestamps (convertendo para um tipo padrão)
    timestamp(pickup_datetime) as data_hora_embarque,
    timestamp(dropoff_datetime) as data_hora_desembarque,
    
    -- Colunas de Flag (convertendo para booleano)
    case 
        when CAST(store_and_fwd_flag AS STRING) = 'N' then false
        when CAST(store_and_fwd_flag AS STRING) = 'Y' then true
        else null 
    end as flag_armazenado_em_memoria,
    
    -- Métricas
    passenger_count as contagem_passageiros,
    trip_duration as duracao_viagem_segundos,
    
    -- Coordenadas (bom para plots no BI)
    pickup_latitude as latitude_embarque,
    pickup_longitude as longitude_embarque,
    dropoff_latitude as latitude_desembarque,
    dropoff_longitude as longitude_desembarque

from
    {{ source('raw', 'train') }}

where
    -- Regra de negócio simples: remover viagens com 0 passageiros
    passenger_count > 0