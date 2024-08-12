
with cte_colleague_data_event as (
    select *
    from {{ ref('stg_colleague_data_event_avro') }}
    where 1=1
    and cast(metadata_time as date) >= '2024-05-09' -- TBC and cast(metadata_time as date) <= '2024-07-15' 
    and try_to_numeric(to_varchar(data_orgunitref_id )) is not null 
)
select 
cast(data_colleaguestatus as varchar) as colleague_status
, cast(data_contractedhours as decimal(5,2)) as contracted_hours
, cast(data_primarycostcenter_id as varchar) as cost_center_id
, cast(data_primarycostcenter_description as varchar) as cost_center_description
, cast(data_countrygroup_id as varchar) as country_group_id 
, cast(data_countrygroup_description as varchar) as country_group_description   
, cast(data_emailaddress as varchar) as email_address

, cast(data_companycode_id as varchar) as company_code_id                    
, cast(data_companycode_description as varchar) as company_code_description      
, cast(data_employeetype_id as varchar) as employee_type_id                     
, cast(data_employeetype_description as varchar) as employee_type_description           

, cast(data_fullname as varchar) as full_name
, cast(data_fulltimeequivalent as decimal(5,2)) as full_time_equivalent
, try_to_date(cast(data_hiredate as varchar)) as hire_date
, cast(data_holidayzone_id as varchar) as holiday_zone_id 
, cast(data_holidayzone_description as varchar) as holiday_zone_description

, cast(data_id as integer) as id
, cast(data_initials as varchar) as initials 
, cast(data_isfinancialadviser as boolean) as is_financial_advisor       
, cast(data_isfirewarden as boolean) as is_fire_warden
, cast(data_isfirstaidofficer as boolean) as is_first_aid_officer
, cast(data_ishealthandsafetyrep as boolean) as is_health_and_safety_rep
, cast(data_isnabstaffaccessingbnzsystems as boolean) as is_nab_staff_accessing_bnz_systems
, cast(data_isoperational as boolean) as is_operational
, cast(data_ispeopleleader as boolean) as is_people_leader

, cast(data_legacycolleagueid as varchar) as legacy_colleague_id           
, cast(data_legalname as varchar) as legal_name                             -- TBC not populated

, cast(data_legalfirstname as varchar) as legal_first_name 
, cast(data_legallastname as varchar) as legal_last_name 
, cast(data_legalmiddlename as varchar) as legal_middle_name 
, cast(data_managerref_id as integer) as manager_id
, cast(data_managerref_fullname as varchar) as manager_name
, cast(data_orgunitref_id as integer) as organisation_unit_id
, try_to_date(cast(data_orgassignmentenddate as varchar)) as organisation_assignment_end_date 
, cast(data_positionref_id as integer) as position_id

, cast(data_preferredfirstname as varchar) as preferred_first_name 
, cast(data_preferredlastname as varchar) as preferred_last_name 

, cast(data_timetype_id as varchar) as time_type_id                    
, cast(data_timetype_description as varchar) as time_type_description           

, cast(data_userid as varchar) as user_id 
, cast(data_workcontact_id as varchar) as work_contact_id
, cast(data_workcontact_emailaddress as varchar) as work_contact_email_address
, cast(data_workcontact_faxnumber as varchar) as work_contact_fax_number
, cast(data_workcontact_isupdated as boolean) as work_contact_is_updated
, cast(data_workcontact_updatedat as timestamp) as work_contact_updated_at
, cast(data_workcontact_landlineextension as varchar) as work_contact_landline_extension
, cast(data_workcontact_landlinenumber as varchar) as work_contact_landline_number
, cast(data_workcontact_mobilenumber as varchar) as work_contact_mobile_number
, cast(data_worklocation_id as varchar) as work_location_id 
, cast(data_worklocation_addressline1 as varchar) as work_location_address_line1
, cast(data_worklocation_addressline2 as varchar) as work_location_address_line2
, cast(data_worklocation_addressline3 as varchar) as work_location_address_line3
, cast(data_worklocation_city as varchar) as work_location_city 
, cast(data_worklocation_country_id as varchar) as work_location_country_id 
, cast(data_worklocation_country_description as varchar) as work_location_country_description 
, cast(data_worklocation_postalcode as varchar) as work_location_postal_code
, cast(data_worklocation_region_id as varchar) as work_location_region_id
, cast(data_worklocation_region_description as varchar) as work_location_region_description  
, cast(data_worklocation_suburb as varchar) as work_location_suburb 
, cast(data_worklocation_isupdated as boolean) as work_location_is_updated
, cast(data_worklocation_updatedat as timestamp) as work_location_updated_at   

, cast(metadata_id as varchar) as event_id
, cast(metadata_type as varchar) as event_type
, to_timestamp(metadata_time) as event_time
, cast(metadata_source as varchar) as event_source
, cast(metadata_subject as varchar) as event_subject
, cast(case when metadata_type <> 'REMOVED' then false else true end as boolean) as Deleted

from cte_colleague_data_event
order by metadata_time desc

