{{
  config(
    materialized = 'incremental',
    transient = False,
    unique_key = 'view_id',
    full_refresh = False,
    tags = ["post_processing"],
    post_hook = ["update {{ this }} set allow_extract = false 
                    where view_id not in (select MD5(table_schema || table_name) as view_id
                    from information_schema.views
                    where table_schema = '{{ target.schema }}' 
                    and upper(table_name) like 'V_EXTRACT%')"]
  )
}}

select MD5(table_schema || table_name) as view_id
, table_schema as view_schema
, table_name as view_name
, created as created_at
, last_altered as modified_at
, cast(1 as boolean) as allow_extract
, '{{ invocation_id }}' || '.' || '{{ model.unique_id }}' as dbt_model_run_id
from information_schema.views
where table_schema = '{{ target.schema }}' 
and upper(table_name) like 'V_EXTRACT%'


{% if is_incremental() %}

and last_altered > (select max(modified_at) from {{ this }})

{% endif %}


