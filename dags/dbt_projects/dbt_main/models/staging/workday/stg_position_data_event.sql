with cte_position_data_event as (
    select *
    from {{ ref('stg_position_data_event_avro') }}
    where 1=1
    and cast(metadata_time as date) >= '2024-05-09' --and cast(metadata_time as date) <= '2024-07-15' 
    and try_to_numeric(to_varchar(data_orgunitref_id )) is not null 
    and try_to_numeric(to_varchar(data_id )) is not null 
)
select 
cast(data_colleaguepositionenddate as date) as colleague_position_end_date
, cast(data_colleaguepositionstartdate as date) as colleague_position_start_date

, cast(data_contact_id as varchar) as contact_id
, cast(data_contact_emailaddress as varchar) as contact_email_address
, cast(data_contact_faxnumber as varchar) as contact_fax_number
, cast(data_contact_isupdated as boolean) as contact_is_updated
, cast(data_contact_updatedat as timestamp) as contact_updated_at
, cast(data_contact_landlineextension as varchar) as contact_landline_extension
, cast(data_contact_landlinenumber as varchar) as contact_landline_number
, cast(data_contact_mobilenumber as varchar) as contact_mobile_number

, cast(data_costcenter_description as varchar) as cost_center_description
, cast(data_costcenter_id as varchar) as cost_center_id
, cast(data_currentincumbentref_id as integer) as current_incumbent_id
, cast(data_currentincumbentref_fullname as varchar) as current_incumbent_name
, cast(data_customerfacingroleflag as boolean) as customer_facing_role_flag
, cast(data_dcaholderflag as boolean) as dca_holder_flag
, cast(data_holidayzone_id as varchar) as holiday_zone_id  
, cast(data_holidayzone_description as varchar) as holiday_zone_description  
, cast(data_id as integer) as id
, cast(data_bankerflag as boolean) as banker_flag
, cast(data_ischiefposition as boolean) as is_chief_position
, cast(data_iswsb as boolean) as is_wsb
, cast(data_ispeopleleader as boolean) as is_people_leader
, cast(data_job_id as varchar) as job_id 
, cast(data_job_jobdescription as varchar) as job_description
, cast(data_joblevel as varchar) as job_level
, cast(data_lendingroleflag as boolean) as lending_role_flag
, cast(data_location_id as varchar) as location_id 
, cast(data_location_addressline1 as varchar) as location_address_line1
, cast(data_location_addressline2 as varchar) as location_address_line2
, cast(data_location_addressline3 as varchar) as location_address_line3
, cast(data_location_city as varchar) as location_city 
, cast(data_location_country_id as varchar) as location_country_id 
, cast(data_location_country_description as varchar) as location_country_description 
, cast(data_location_postalcode as varchar) as location_postal_code
, cast(data_location_region_id as varchar) as location_region_id 
, cast(data_location_region_description as varchar) as location_region_description 
, cast(data_location_suburb as varchar) as location_suburb 
, cast(data_location_isupdated as boolean) as location_is_updated
, cast(data_location_updatedat as timestamp) as location_updated_at 
, cast(data_managecustomerroleflag as boolean) as manage_customer_role_flag
, cast(data_nominatedrepresentativeflag as boolean) as nominated_representative_flag
, cast(data_orgunitref_id as integer) as organisation_unit_id

, cast(data_positionbusinessdescription as varchar) as position_business_description
, cast(data_positiondescription as varchar) as position_description
, cast(data_positionenddate as date) as position_end_date
, cast(data_positionstartdate as date) as position_start_date
, cast(data_purchasingapprovallevel as integer) as purchasing_approval_level
, cast(data_responsiblepersonflag as boolean) as responsible_person_flag
, cast(data_storebasedroleflag as boolean) as store_based_role_flag

, cast(data_companycode_id as varchar) as company_code_id 
, cast(data_companycode_description as varchar) as company_code_description 
, cast(metadata_id as varchar) as event_id
, cast(metadata_type as varchar) as event_type
, to_timestamp(metadata_time) as event_time
, cast(metadata_source as varchar) as event_source
, cast(metadata_subject as varchar) as event_subject
, cast(case when metadata_type <> 'REMOVED' then false else true end as boolean) as deleted
from cte_position_data_event


