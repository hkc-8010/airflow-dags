{{
    config(
        materialized='incremental',
        unique_key='dbt_scd_id',
        merge_exclude_columns = ['created_at']
    )
}}
 
select id
, company_code_id
, contact_id
, location_id
, region_id
, country_id
, cost_center_id
, event_id
, event_type
, event_time
, event_source
, event_subject
, current_timestamp() as created_at
, current_timestamp() as modified_at
, md5(concat(coalesce(cast(id as string),''), coalesce(cast(to_timestamp(event_time) as string),''))) as dbt_scd_id
, '{{ invocation_id }}' || '.' || '{{ model.unique_id }}' as dbt_model_run_id
from {{ ref('stg_orgunit_data_event') }}
 
 
{% if is_incremental() %}
 
where
  event_time > (select max(event_time) from {{ this }})
 
{% endif %}