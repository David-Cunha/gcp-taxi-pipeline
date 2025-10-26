{{ config(materialized='table') }}

select
    -- Agora usamos a coluna descritiva da Dimensão!
    dim.nome_fornecedor,

    -- E agregamos as métricas da Fato
    count(fct.id_viagem) as total_viagens,
    avg(fct.duracao_viagem_segundos) as media_duracao_viagem_seg,
    avg(fct.contagem_passageiros) as media_passageiros

from
    -- 1. Começamos pela tabela Fato
    {{ ref('fct_trips') }} as fct

left join 
    -- 2. Fazemos o JOIN com a tabela Dimensão
    {{ ref('dim_vendor') }} as dim

    -- 3. Usamos a chave em comum
    on fct.id_fornecedor = dim.id_fornecedor

group by
    -- Agrupamos pelo nome descritivo
    1   