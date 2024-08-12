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
{% set column_names =  ['id', 'organisation_unit_description', 'organisation_unit_level', 'business_unit_id', 'business_unit_description', 'parent_organisation_unit_id', 'organisation_unit_level1_id', 'organisation_unit_level1_description', 'organisation_unit_level2_id', 'organisation_unit_level2_description', 'organisation_unit_level3_id', 'organisation_unit_level3_description', 'organisation_unit_level4_id', 'organisation_unit_level4_description', 'organisation_unit_level5_id', 'organisation_unit_level5_description', 'organisation_unit_level6_id', 'organisation_unit_level6_description', 'organisation_unit_level7_id', 'organisation_unit_level7_description', 'organisation_unit_level8_id', 'organisation_unit_level8_description', 'organisation_unit_level9_id', 'organisation_unit_level9_description','organisation_unit_level10_id','organisation_unit_level10_description','organisation_unit_level11_id','organisation_unit_level11_description','Deleted'] %}

with source_rows_base as (
    select id
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
    , event_id
    , event_time
    , 'orgunit' as event_msg_source
    , current_timestamp() as created_at
    , current_timestamp() as modified_at
    , deleted
    from {{ ref('stg_orgunit_data_event') }}
    where id is not null

)

{{ generate_scd_incremental(column_names, is_incremental()) }}
