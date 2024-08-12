{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='location_key',
        tags = ["main_processing"],
        merge_exclude_columns = ['created_at']
    )
}}


select dbt_scd_id as location_key
, id as location_id
, address_line1
, address_line2
, address_line3  
, suburb
, city
, postal_code
, region_id
, region
, country_id
, country
, updated_at as location_updated_at
, is_updated as location_updated_flag
, dbt_valid_from
, dbt_valid_to
, coalesce(modified_at, created_at) as dbt_updated_at
, current_timestamp() as created_at
, current_timestamp() as modified_at
, '{{ invocation_id }}' || '.' || '{{ model.unique_id }}' as dbt_model_run_id
from {{ ref('stg_location') }}

{% if is_incremental() %}
where coalesce(modified_at, created_at) > (select max(modified_at) from {{ this }})
{% endif %}



