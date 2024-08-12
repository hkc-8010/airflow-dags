{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='organisation_unit_key',
        tags = ["main_processing"],
        merge_exclude_columns = ['created_at']
    )
}}


select  dbt_scd_id as organisation_unit_key
, id as organisation_unit_id
, organisation_unit_description
, organisation_unit_level
, business_unit_id
, business_unit_description
, parent_organisation_unit_id
, organisation_unit_level1_id
, organisation_unit_level1_description
, organisation_unit_level2_id
, organisation_unit_level2_description
, organisation_unit_level3_id
, organisation_unit_level3_description
, organisation_unit_level4_id
, organisation_unit_level4_description
, organisation_unit_level5_id
, organisation_unit_level5_description
, organisation_unit_level6_id
, organisation_unit_level6_description
, organisation_unit_level7_id
, organisation_unit_level7_description
, organisation_unit_level8_id
, organisation_unit_level8_description
, organisation_unit_level9_id
, organisation_unit_level9_description
, organisation_unit_level10_id
, organisation_unit_level10_description
, organisation_unit_level11_id
, organisation_unit_level11_description
, dbt_valid_from
, dbt_valid_to
, coalesce(modified_at, created_at) as dbt_updated_at
, current_timestamp() as created_at
, current_timestamp() as modified_at
, deleted
, '{{ invocation_id }}' || '.' || '{{ model.unique_id }}' as dbt_model_run_id
from {{ ref('stg_organisation_unit') }}  

{% if is_incremental() %}
where coalesce(modified_at, created_at) > (select max(modified_at) from {{ this }})
{% endif %}
