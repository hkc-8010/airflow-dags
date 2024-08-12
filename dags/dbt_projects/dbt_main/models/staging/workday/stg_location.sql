
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
{% set column_names =  ['id', 'address_line1', 'address_line2', 'address_line3', 'suburb', 'city', 'postal_code', 'region_id', 'region', 'country_id', 'country', 'is_updated', 'updated_at'] %}

with source_rows_base as (
    select location_id as id    
    , location_address_line1 as address_line1
    , location_address_line2 as address_line2
    , location_address_line3 as address_line3
    , location_suburb as suburb
    , location_city as city
    , location_postal_code as postal_code
    , location_region_id as region_id
    , location_region_description as region
    , location_country_id as country_id
    , location_country_description as country
    , location_is_updated as is_updated
    , location_updated_at as updated_at
    , event_id
    , event_time
    , 'position' as event_msg_source
    from {{ ref('stg_position_data_event') }}
    where location_id is not null

    union

    select location_id as id    
    , location_address_line1
    , location_address_line2
    , location_address_line3
    , location_suburb as suburb
    , location_city as city
    , location_postal_code
    , region_id
    , region_description as region
    , country_id
    , country_description as country
    , location_is_updated as is_updated
    , location_updated_at as updated_at
    , event_id
    , event_time
    , 'orgunit' as event_msg_source
    from {{ ref('stg_orgunit_data_event') }}
    where location_id is not null

    union

    select home_location_id as id
	, home_location_address_line1
	, home_location_address_line2
	, home_location_address_line3
    , home_location_suburb
	, home_location_city
    , home_location_postal_code
    , home_location_region_id
    , home_location_region_description
    , home_location_country_id
    , home_location_country_description
	, home_location_is_updated
	, home_location_updated_at
    , event_id
    , event_time
    , 'colleague_confidential' as event_msg_source
    from {{ ref('stg_colleague_confidential_data_event') }}
    where home_location_id is not null

    union

    select work_location_id as id
	, work_location_address_line1
	, work_location_address_line2
	, work_location_address_line3
    , work_location_suburb
	, work_location_city
    , work_location_postal_code
    , work_location_region_id
    , work_location_region_description
    , work_location_country_id
    , work_location_country_description
	, work_location_is_updated
	, work_location_updated_at
    , event_id
    , event_time
    , 'colleague' as event_msg_source
    from {{ ref('stg_colleague_data_event') }}
    where work_location_id is not null

)


{{ generate_scd_incremental(column_names, is_incremental()) }}












