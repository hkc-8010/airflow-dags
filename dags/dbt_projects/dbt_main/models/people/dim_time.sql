
{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='time_key',
        tags = ["pre_processing"]
       
    )
}}

with cte_time as(
select dateadd(seconds, seq4(), '2024-01-01 00:00:00') AS time_of_day
from table(generator(rowcount=>86400))
)
select
    md5(to_char(to_timestamp(time_of_day), 'HH:MI:SS')) as time_key
    ,to_time(time_of_day)  as time_of_day
    ,hour(time_of_day) as hour
    ,minute(time_of_day) as minute
    ,second(time_of_day) as second
    , current_timestamp() as created_at
from cte_time

