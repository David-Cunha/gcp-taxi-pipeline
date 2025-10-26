{{ config(materialized='table') }}

select
    id_fornecedor,

    -- Lógica de negócio para traduzir o ID em um nome
    case 
        when id_fornecedor = 1 then 'Creative Mobile Technologies'
        when id_fornecedor = 2 then 'VeriFone Inc.'
        else 'Desconhecido' 
    end as nome_fornecedor

from 
    -- Usamos 'ref' para ler da nossa camada 'refined' (prata)
    {{ ref('stg_nyc_taxi_train') }}

-- Agrupamos para garantir que temos apenas um registro por ID
group by 1, 2