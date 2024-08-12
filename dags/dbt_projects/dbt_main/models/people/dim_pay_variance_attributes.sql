{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='pay_variance_attributes_key',
        tags = ["main_processing"],
        merge_exclude_columns = ['created_at']
    )
}}


select dbt_scd_id as pay_variance_attributes_key
, id as pay_variance_attributes_id
, included_in_gross
, finalised 
, retro
, dbt_valid_from
, dbt_valid_to
, coalesce(modified_at, created_at) as dbt_updated_at
, current_timestamp() as created_at
, current_timestamp() as modified_at
, '{{ invocation_id }}' || '.' || '{{ model.unique_id }}' as dbt_model_run_id
from {{ ref('stg_pay_variance_attributes') }}

{% if is_incremental() %}
where coalesce(modified_at, created_at) > (select max(modified_at) from {{ this }})
{% endif %}
