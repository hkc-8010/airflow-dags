{{
    config(
        materialized='incremental',
        merge_exclude_columns = ['created_at']
    )
}}


{# /* get list of files that we haven't processed yet. NB. assumes no subfolder */ #}
{% set file_list %}
    select split_part(filepath, '/', -1) 
    from {{ ref('raw_stage_list') }} 
    where filepath like '%PAYVAR%'
    
    {% if is_incremental() %}
        and split_part(filepath, '/', -1) not in(select distinct file_name from {{ this }} )
    {% endif %}
{% endset %}

{% set result = run_query(file_list) %}
{% if execute %}
    {% set res_list = result.columns[0].values() %}
{% else %}
    {% set res_list = [] %}
{% endif %}

{# /* build our union-ised dataset */ #}
select cast(null as integer) as employee_id
, cast(null as varchar) as wage_type_code 
, cast(null as varchar) as wage_type_description
, cast(null as varchar) as included_in_gross 
, cast(null as float) as pay_quantity 
, cast(null as varchar) as pay_unit_type 
, cast(null as numeric) as pay_amount 
, cast(null as date) as pay_period_start_date
, cast(null as date) as pay_period_end_date
, cast(null as varchar) as finalised
, cast(null as varchar) as retro
, cast(null as date) as for_pay_period_start_date
, cast(null as date) as for_pay_period_end_date
, cast(null as varchar) as file_name
, cast(null as timestamp) as file_time
, cast(null as timestamp) as created_at
, cast(null as timestamp) as modified_at
, cast(null as varchar) as id
, '{{ invocation_id }}' || '.' || '{{ model.unique_id }}' as dbt_model_run_id
where 1 =0

{% for i in res_list %}
union
select t.$1::integer as employee_id
, t.$2::varchar as wage_type_code 
, t.$3::varchar as wage_type_description
, t.$4::varchar as included_in_gross 
, t.$5::float as pay_quantity 
, t.$6::varchar as pay_unit_type 
, t.$7::numeric as pay_amount 
, try_to_date(cast(right(t.$8,4) || '-' || substring(t.$8,4,2) || '-' || left(t.$8,2) as varchar)) as pay_period_start_date
, try_to_date(cast(right(t.$9,4) || '-' || substring(t.$9,4,2) || '-' || left(t.$9,2) as varchar)) as pay_period_end_date
, t.$10::varchar as finalised
, t.$11::varchar as retro
, try_to_date(cast(right(t.$12,4) || '-' || substring(t.$12,4,2) || '-' || left(t.$12,2) as varchar)) as for_pay_period_start_date
, try_to_date(cast(right(t.$13,4) || '-' || substring(t.$13,4,2) || '-' || left(t.$13,2) as varchar)) as for_pay_period_end_date
, split_part(METADATA$FILENAME, '/', -1) as filename
, try_to_timestamp(replace(right(split_part(METADATA$FILENAME, '/', -1), 18), '.DAT', '') , 'YYYYMMDDHHMISS') as file_time      -- TBC .DAT case?
, current_timestamp() as created_at
, current_timestamp() as modified_at
, md5(concat(coalesce(cast(t.$1 as string),'')
                            || coalesce(cast(t.$2 as string),'')
                            || coalesce(cast(t.$5 as string),'')
                            || coalesce(cast(t.$7 as string),'')
                            || coalesce(cast(t.$8 as string),'')
                            || coalesce(cast(t.$9 as string),'')
                            || coalesce(cast(t.$10 as string),'')
                            || coalesce(cast(t.$11 as string),'')
                            || coalesce(cast(t.$12 as string),'')
                            || coalesce(cast(t.$13 as string),'')
                            || coalesce(split_part(METADATA$FILENAME, '/', -1),'')
            )               
) as id 
, '{{ invocation_id }}' || '.' || '{{ model.unique_id }}' as dbt_model_run_id          
from @people_raw_stage/{{i}} t

{% endfor %}





