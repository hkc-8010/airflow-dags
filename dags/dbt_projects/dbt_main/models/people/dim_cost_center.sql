{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='cost_center_key',
        tags = ["main_processing"],
        merge_exclude_columns = ['created_at']
    )
}}


select dbt_scd_id as cost_center_key
, id as cost_center_id
, cost_center_description  
, dbt_valid_from
, dbt_valid_to
, coalesce(modified_at, created_at) as dbt_updated_at
, current_timestamp() as created_at
, current_timestamp() as modified_at
, '{{ invocation_id }}' || '.' || '{{ model.unique_id }}' as dbt_model_run_id
from {{ ref('stg_cost_center') }}

{% if is_incremental() %}
where coalesce(modified_at, created_at) > (select max(modified_at) from {{ this }})
{% endif %}