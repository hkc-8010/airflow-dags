{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='contact_key',
        tags = ["main_processing"],
        merge_exclude_columns = ['created_at']
    )
}}


select dbt_scd_id as contact_key
, id as contact_id
, email_address
, mobile_number
, land_line_number
, land_line_extension
, fax_number
, updated_at as contact_updated_at
, is_updated as contact_updated_flag
, dbt_valid_from
, dbt_valid_to
, coalesce(modified_at, created_at) as dbt_updated_at
, current_timestamp() as created_at
, current_timestamp() as modified_at
, '{{ invocation_id }}' || '.' || '{{ model.unique_id }}' as dbt_model_run_id
from {{ ref('stg_contact') }}

{% if is_incremental() %}
where coalesce(modified_at, created_at) > (select max(modified_at) from {{ this }})
{% endif %}
