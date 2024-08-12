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
-- NB. this scd incremental approach relies on the key of your table named as "id" and the second column to be the main descriptive attribute (this will be used for Unknown member in the data product dimension)
{% set column_names =  ['id', 'position_description', 'position_business_description', 'position_start_date', 'position_end_date', 'colleague_position_start_date', 'colleague_position_end_date', 'banker_flag', 'is_chief_position', 'is_wsb', 'is_people_leader', 'lending_role_flag', 'customer_facing_role_flag', 'dca_holder_flag', 'store_based_role_flag', 'manage_customer_role_flag', 'nominated_representative_flag', 'responsible_person_flag', 'purchasing_approval_level', 'job_level', 'deleted'] %}

with source_rows_base as (
  select id
  , position_description
  , position_business_description
  , position_start_date
  , position_end_date
  , colleague_position_start_date
  , colleague_position_end_date
  , banker_flag
  , is_chief_position
  , is_wsb
  , is_people_leader
  , lending_role_flag
  , customer_facing_role_flag
  , dca_holder_flag
  , store_based_role_flag
  , manage_customer_role_flag
  , nominated_representative_flag
  , responsible_person_flag
  , purchasing_approval_level
  , job_level
  , event_id
  , event_time
  , 'position' as event_msg_source
  , current_timestamp() as created_at
  , current_timestamp() as modified_at
  , deleted
  from {{ ref('stg_position_data_event') }}

)

{{ generate_scd_incremental(column_names, is_incremental()) }}

