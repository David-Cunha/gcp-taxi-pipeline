{{ config(materialized='table') }}

select
    data_hora_embarque as id_datetime, -- Usamos o timestamp como a chave única
    extract(HOUR from data_hora_embarque) as hora_do_dia,
    extract(DAYOFWEEK from data_hora_embarque) as dia_da_semana_num,
    case 
        when extract(DAYOFWEEK from data_hora_embarque) = 1 then 'Domingo'
        when extract(DAYOFWEEK from data_hora_embarque) = 2 then 'Segunda'
        when extract(DAYOFWEEK from data_hora_embarque) = 3 then 'Terça'
        when extract(DAYOFWEEK from data_hora_embarque) = 4 then 'Quarta'
        when extract(DAYOFWEEK from data_hora_embarque) = 5 then 'Quinta'
        when extract(DAYOFWEEK from data_hora_embarque) = 6 then 'Sexta'
        when extract(DAYOFWEEK from data_hora_embarque) = 7 then 'Sábado'
    end as dia_da_semana_nome,
    extract(MONTH from data_hora_embarque) as mes,
    extract(YEAR from data_hora_embarque) as ano

from
    {{ ref('stg_nyc_taxi_train') }}

-- Agrupamos para ter apenas um registro único por timestamp
group by 1