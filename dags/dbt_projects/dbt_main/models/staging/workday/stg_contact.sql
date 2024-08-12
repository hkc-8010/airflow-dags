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
{% set column_names =  ['id', 'email_address', 'mobile_number', 'land_line_number' , 'land_line_extension' ,'fax_number' , 'updated_at' , 'is_updated'] %}
 
with source_rows_base as (
    select contact_id as id
    , contact_email_address as email_address
    , contact_mobile_number as mobile_number
    , contact_landline_number as land_line_number
    , contact_landline_extension as land_line_extension
    , contact_fax_number as fax_number
    , contact_updated_at as updated_at
    , contact_is_updated_flag as is_updated
    , event_id
    , event_time
    , 'orgunit' as event_msg_source
    from {{ ref('stg_orgunit_data_event') }}
    where contact_id is not null

    union

    select work_contact_id as id
    , work_contact_email_address as email_address
    , work_contact_mobile_number as mobile_number
    , work_contact_landline_number as land_line_number
    , work_contact_landline_extension as land_line_extension
    , work_contact_fax_number as fax_number
    , work_contact_updated_at as updated_at
    , work_contact_is_updated as is_updated_flag
    , event_id
    , event_time
    , 'colleague' as event_msg_source
    from {{ ref('stg_colleague_data_event') }}
    where work_contact_id is not null

    union

    select emergency_contact_id as id
    , emergency_contact_email_address as email_address
    , emergency_contact_mobile_number as mobile_number
    , emergency_contact_landline_number as land_line_number
    , emergency_contact_landline_extension as land_line_extension
    , emergency_contact_fax_number as fax_number
    , emergency_contact_updated_at as updated_at
    , emergency_contact_is_updated as is_updated_flag
    , event_id
    , event_time
    , 'colleague_confidential' as event_msg_source
    from {{ ref('stg_colleague_confidential_data_event') }}
    where emergency_contact_id is not null

    union

    select home_contact_id as id
    , home_contact_email_address as email_address
    , home_contact_mobile_number as mobile_number
    , home_contact_landline_number as land_line_number
    , home_contact_landline_extension as land_line_extension
    , home_contact_fax_number as fax_number
    , home_contact_updated_at as updated_at
    , home_contact_is_updated as is_updated_flag
    , event_id
    , event_time
    , 'colleague_confidential' as event_msg_source
    from {{ ref('stg_colleague_confidential_data_event') }}
    where home_contact_id is not null

    union

    select contact_id as id
    , contact_email_address as email_address
    , contact_mobile_number as mobile_number
    , contact_landline_number as land_line_number
    , contact_landline_extension as land_line_extension
    , contact_fax_number as fax_number
    , contact_updated_at as updated_at
    , contact_is_updated as is_updated
    , event_id
    , event_time
    , 'position' as event_msg_source
    from {{ ref('stg_position_data_event') }}
    where contact_id is not null

)
 
 
{{ generate_scd_incremental(column_names, is_incremental()) }}
 