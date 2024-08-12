with cte_cc_data_event as (
    select *
    from {{ ref('stg_colleague_confidential_data_event_avro') }}
    where 1=1
    and cast(metadata_time as date) >= '2024-05-09' --and cast(metadata_time as date) <= '2024-07-15'   -- TBC
)
select distinct
cast(data_id as integer) as id
, try_to_date(cast(data_action_actionbegindate as varchar)) as action_begin_date
, try_to_date(cast(data_action_actionchangeddate as varchar)) as action_changed_date
, cast(data_action_actionreasondescription as varchar) as action_reason_description
, cast(data_action_actionreasonid as varchar) as action_reason_id
, cast(data_action_actiontypedescription as varchar) as action_type_description
, cast(data_action_actiontypeid as varchar) as action_type_id
, cast(replace(data_annualleavebalance,',','') as number(15,2)) as annual_leave_balance
, cast(replace(data_annualsalaryamount,',','') as number(15,2)) as annual_salary_amount
, try_to_date(cast(data_compensationstartdate as varchar)) as compensation_start_date
, try_to_date(cast(data_contractexpirydate as varchar)) as contract_expiry_date
, cast(data_contractgroup as varchar) as contract_group
, try_to_date(cast(data_contractstartdate as varchar)) as contract_start_date
, cast(data_contracttype_description as varchar) as contract_type_description
, cast(data_contracttype_id as varchar) as contract_type_id

, try_to_date(cast(data_dateofbirth as varchar)) as date_of_birth
, cast(data_emergencycontact_id as varchar) as emergency_contact_id
, cast(data_emergencycontact_emailaddress as varchar) as emergency_contact_email_address
, cast(data_emergencycontact_faxnumber as varchar) as emergency_contact_fax_number
, cast(data_emergencycontact_isupdated as boolean) as emergency_contact_is_updated
, cast(data_emergencycontact_updatedat as timestamp) as emergency_contact_updated_at
, cast(data_emergencycontact_landlineextension as varchar) as emergency_contact_landline_extension
, cast(data_emergencycontact_landlinenumber as varchar) as emergency_contact_landline_number
, cast(data_emergencycontact_mobilenumber as varchar) as emergency_contact_mobile_number
, cast(data_genderidentity as varchar) as gender_identity
, cast(data_homecontact_id as varchar) as home_contact_id
, cast(data_homecontact_emailaddress as varchar) as home_contact_email_address
, cast(data_homecontact_faxnumber as varchar) as home_contact_fax_number
, cast(data_homecontact_isupdated as boolean) as home_contact_is_updated
, cast(data_homecontact_updatedat as timestamp) as home_contact_updated_at
, cast(data_homecontact_landlineextension as varchar) as home_contact_landline_extension
, cast(data_homecontact_landlinenumber as varchar) as home_contact_landline_number
, cast(data_homecontact_mobilenumber as varchar) as home_contact_mobile_number
, cast(data_homelocation_id as varchar) as home_location_id 
, cast(data_homelocation_addressline1 as varchar) as home_location_address_line1
, cast(data_homelocation_addressline2 as varchar) as home_location_address_line2
, cast(data_homelocation_addressline3 as varchar) as home_location_address_line3
, cast(data_homelocation_city as varchar) as home_location_city 
, cast(data_homelocation_country_id as varchar) as home_location_country_id 
, cast(data_homelocation_country_description as varchar) as home_location_country_description 
, cast(data_homelocation_postalcode as varchar) as home_location_postal_code
, cast(data_homelocation_region_id as varchar) as home_location_region_id        
, cast(data_homelocation_region_description as varchar) as home_location_region_description   
, cast(data_homelocation_suburb as varchar) as home_location_suburb 
, cast(data_homelocation_isupdated as boolean) as home_location_is_updated
, cast(data_homelocation_updatedat as timestamp) as home_location_updated_at 

, cast(replace(data_kiwisaveremployeecontribution,',','') as number(15,2)) as kiwisaver_employee_contribution
, cast(replace(data_kiwisaveremployercontribution,',','') as number(15,2)) as kiwisaver_employer_contribution
, cast(data_paycurrency as varchar) as pay_currency


, cast(data_managersmanagerref_id as integer) as managers_manager_id
, cast(data_payscalelevel as varchar) as pay_scale_level                -- TBC check data type when we have data

, cast(data_simplifiedgroup as varchar) as simplified_group 

, cast(replace(data_superannuationemployeecontribution,',','') as number(15,2)) as superannuation_employee_contribution
, cast(replace(data_superannuationemployercontribution,',','') as number(15,2)) as superannuation_employer_contribution

, cast(metadata_id as varchar) as event_id
, cast(metadata_type as varchar) as event_type
, to_timestamp(metadata_time) as event_time
, cast(metadata_source as varchar) as event_source
, cast(metadata_subject as varchar) as event_subject
, cast(case when metadata_type <> 'REMOVED' then false else true end as boolean) as Deleted
from cte_cc_data_event