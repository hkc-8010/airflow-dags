
{{ 
    config(
        materialized='incremental',
        unique_key="md5(concat(coalesce(cast(id as string),''), coalesce(cast(to_timestamp(event_time) as string),'')))",
        incremental_strategy='delete+insert',
        merge_exclude_columns=['created_at'],
        incremental_predicates=["dbt_valid_to is null"],
        post_hook=["update {{ this }} set modified_at=current_timestamp() where modified_at is null and dbt_valid_to is not null"]
    ) 
}}

-- specify your column names from source_rows_base (not including the event meta) so we can inject into the re-useable logic housed in generate_scd_incremental...
-- NB. this scd incremental approach relies on the key of your table named as "id"
{% set column_names =  ['id', 'action_type_id', 'action_type_description', 'action_reason_id', 'action_reason_description'] %}

with source_rows_base as (
    select action_type_id || '_' || action_reason_id as id
    , action_type_id
    , action_type_description  
    , action_reason_id
    , action_reason_description
    , event_id
    , event_time
    , 'colleague_confidential' as event_msg_source
     from {{ ref('stg_colleague_confidential_data_event') }}
     where action_type_id is not null  
     and action_reason_id is not null
)

{{ generate_scd_incremental(column_names, is_incremental()) }}