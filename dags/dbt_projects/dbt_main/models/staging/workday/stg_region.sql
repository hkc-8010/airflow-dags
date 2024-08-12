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
{% set column_names =  ['id', 'region_description'] %}
 
with source_rows_base as (
    select region_id as id
    , region_description
    , event_id
    , event_time
    , 'orgunit' as event_msg_source
    from {{ ref('stg_orgunit_data_event') }}
    where region_id is not null

    union

    select location_region_id as id
    , location_region_description
    , event_id
    , event_time
    , 'position' as event_msg_source
    from {{ ref('stg_position_data_event') }}
    where location_region_id is not null

    union

    select home_location_region_id as id
    , home_location_region_description
    , event_id
    , event_time
    , 'colleague_confidential' as event_msg_source
    from {{ ref('stg_colleague_confidential_data_event') }}
    where home_location_region_id is not null

    union

    select work_location_region_id as id
    , work_location_region_description
    , event_id
    , event_time
    , 'colleague' as event_msg_source
    from {{ ref('stg_colleague_data_event') }}
    where work_location_region_id is not null

)
 
 
{{ generate_scd_incremental(column_names, is_incremental()) }}
 