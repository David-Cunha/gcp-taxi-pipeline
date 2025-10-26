{{ config(materialized='table') }}

select
    -- Usamos os nomes limpos da camada 'refined'
    id_fornecedor,
    count(id_viagem) as total_viagens,
    avg(duracao_viagem_segundos) as media_duracao_viagem_seg,
    avg(contagem_passageiros) as media_passageiros

from
    -- Note que agora usamos 'ref' para nos referirmos ao nosso OUTRO modelo dbt
    {{ ref('stg_nyc_taxi_train') }}

group by
    1