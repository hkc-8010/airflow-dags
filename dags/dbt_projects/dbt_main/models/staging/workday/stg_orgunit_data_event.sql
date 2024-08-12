with cte_orgunit_data_event as (
    select *
    from {{ ref('stg_orgunit_data_event_avro') }} o
    where cast(o.METADATA_TIME as date) >= '2024-05-09' --and cast(o.metadata_time as date) <= '2024-07-15'       -- TBC
    and try_to_numeric(to_varchar(o.data_id)) is not null  -- TBC to stop the dodgy values from Workday Asia coming in
    and (try_to_numeric(to_varchar(o.data_orgunitlevel1ref_id)) is not null or o.data_orgunitlevel1ref_id is null)
    and (try_to_numeric(to_varchar(o.data_orgunitlevel2ref_id)) is not null or o.data_orgunitlevel2ref_id is null)
    and (try_to_numeric(to_varchar(o.data_orgunitlevel3ref_id)) is not null or o.data_orgunitlevel3ref_id is null)
    and (try_to_numeric(to_varchar(o.data_orgunitlevel4ref_id)) is not null or o.data_orgunitlevel4ref_id is null)
    and (try_to_numeric(to_varchar(o.data_orgunitlevel5ref_id)) is not null or o.data_orgunitlevel5ref_id is null)
    and (try_to_numeric(to_varchar(o.data_orgunitlevel6ref_id)) is not null or o.data_orgunitlevel6ref_id is null)
    and (try_to_numeric(to_varchar(o.data_orgunitlevel7ref_id)) is not null or o.data_orgunitlevel7ref_id is null)
    and (try_to_numeric(to_varchar(o.data_orgunitlevel8ref_id)) is not null or o.data_orgunitlevel8ref_id is null)
    and (try_to_numeric(to_varchar(o.data_orgunitlevel9ref_id)) is not null or o.data_orgunitlevel9ref_id is null)
    and (try_to_numeric(to_varchar(o.data_orgunitlevel10ref_id)) is not null or o.data_orgunitlevel10ref_id is null)
    and (try_to_numeric(to_varchar(o.data_orgunitlevel11ref_id)) is not null or o.data_orgunitlevel11ref_id is null)
    and (try_to_numeric(to_varchar(o.data_orgunitlevel12ref_id)) is not null or o.data_orgunitlevel12ref_id is null)
)
select
data_id::integer as id, 
data_description::varchar as organisation_unit_description, 
data_orgunitlevel::integer as organisation_unit_level,
DATA_PARENTORGUNITREF_ID::integer as parent_organisation_unit_id,
data_businessunitid::integer as business_unit_id,
data_businessunitdescription::varchar as business_unit_description,

data_companycode_id::varchar as company_code_id,
data_companycode_description::varchar as company_code_description,

data_costcenter_description as cost_center_description,
data_costcenter_id::varchar as cost_center_id,

data_contact_id::varchar as contact_id, 
data_contact_emailaddress::varchar as contact_email_address, 
data_contact_faxnumber::varchar as contact_fax_number, 
data_contact_isupdated::boolean as contact_is_updated_flag, 
data_contact_landlineextension::varchar as contact_landline_extension, 
data_contact_landlinenumber::varchar as contact_landline_number, 
data_contact_mobilenumber::varchar as contact_mobile_number, 
data_contact_updatedat::timestamp as contact_updated_at, 

data_location_id::varchar as location_id, 
data_location_addressline1::varchar as location_address_line1, 
data_location_addressline2::varchar as location_address_line2, 
data_location_addressline3::varchar as location_address_line3, 
data_location_city::varchar as location_city, 
data_location_country_id::varchar as country_id, 
data_location_country_description::varchar as country_description, 
data_location_isupdated::varchar as location_is_updated, 
data_location_postalcode::varchar as location_postal_code, 
data_location_region_id::varchar as region_id,
data_location_region_description::varchar as region_description,
data_location_suburb::varchar as location_suburb,
data_location_updatedat::varchar as location_updated_at,

data_orgunitlevel1ref_description::varchar as organisation_unit_level1_description, 
data_orgunitlevel1ref_id::integer as organisation_unit_level1_id, 
data_orgunitlevel2ref_description::varchar as organisation_unit_level2_description, 
data_orgunitlevel2ref_id::integer as organisation_unit_level2_id, 
data_orgunitlevel3ref_description::varchar as organisation_unit_level3_description, 
data_orgunitlevel3ref_id::integer as organisation_unit_level3_id, 
data_orgunitlevel4ref_description::varchar as organisation_unit_level4_description, 
data_orgunitlevel4ref_id::integer as organisation_unit_level4_id, 
data_orgunitlevel5ref_description::varchar as organisation_unit_level5_description, 
data_orgunitlevel5ref_id::integer as organisation_unit_level5_id,
data_orgunitlevel6ref_description::varchar as organisation_unit_level6_description,
data_orgunitlevel6ref_id::integer as organisation_unit_level6_id,
data_orgunitlevel7ref_description::varchar as organisation_unit_level7_description,
data_orgunitlevel7ref_id::integer as organisation_unit_level7_id,
data_orgunitlevel8ref_description::varchar as organisation_unit_level8_description,
data_orgunitlevel8ref_id::integer as organisation_unit_level8_id,
data_orgunitlevel9ref_description::varchar as organisation_unit_level9_description,
data_orgunitlevel9ref_id::integer as organisation_unit_level9_id,
data_orgunitlevel10ref_description::varchar as organisation_unit_level10_description,
data_orgunitlevel10ref_id::integer as organisation_unit_level10_id,
data_orgunitlevel11ref_description::varchar as organisation_unit_level11_description,
data_orgunitlevel11ref_id::integer as organisation_unit_level11_id,
data_orgunitlevel12ref_description::varchar as organisation_unit_level12_description,
data_orgunitlevel12ref_id::integer as organisation_unit_level12_id,

metadata_id::varchar event_id,
metadata_type::varchar as event_type,
metadata_time::TIMESTAMP as event_time,
metadata_source::varchar as event_source,
metadata_subject::varchar as event_subject,
cast(case when ( metadata_type::varchar) <> 'REMOVED' then false else true end as boolean) as Deleted
from cte_orgunit_data_event