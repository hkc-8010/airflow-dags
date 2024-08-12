with cte_fact_first_pass as (
    select f.*, c.colleague_id, c.deleted
    from {{ ref('fact_colleague') }} f
    inner join {{ ref('dim_colleague') }} c on f.colleague_key = c.colleague_key
    inner join {{ ref('dim_date') }} d on d.date_key = f.date_key
    inner join {{ ref('dim_time') }} t on t.time_key = f.time_key
    qualify row_number() over(partition by c.colleague_id, d.date order by t.time_of_day desc) = 1      -- most recent fact for each day  

), cte_final as (
    select *
    from cte_fact_first_pass
    qualify row_number() over(partition by colleague_id order by event_time desc) = 1           -- most recent day  
)
select c.colleague_id as "Pers.no."
, cast(null as varchar) as "Form-of-Address Key"
, c.preferred_first_name as "Known as"
, c.legal_last_name as "Last name"
, c.legal_first_name as "First name"
, c.legal_middle_name as "Middle name"
, c.initials as "Initials"
, cc.gender_identity as "Gender Key"                                    
, to_varchar(cc.date_of_birth, 'DD.MM.YYYY') as "Birth date"                                   
, to_varchar(c.hire_date, 'DD.MM.YYYY') as "Entry"
, cst.cost_center_id  as "Cost ctr"                                      
, cst.cost_center_description  as "Cost Center"                                   
, o.organisation_unit_id as "Org.unit"
, o.organisation_unit_description as "Organizational Unit"
, to_varchar(c.hire_date, 'DD.MM.YYYY') as "Start date"
, to_varchar(cc.contract_expiry_date, 'DD.MM.YYYY')  as "End date"          
, et.employee_type_id as "EEGrp"                                           
, cc.simplified_group || ' ' || tt.time_type_description  as "Employee Group"                                
, cast(null as varchar)  as "ESgrp"                                           -- TBC contractual
, cast(null as varchar)  as "Employee Subgroup"                             -- TBC contractual
, p.position_id as "Position"
, p.position_description as "PositionDesc"                -- NB. spec says Position as column name resulting in duplicate
, j.job_id as "Job"
, j.job_description as "JobDesc"                          -- NB. spec says Job as column name resulting in duplicate
, wl.location_id as "Organizational key"    
, cast(null as varchar)  as "Ty."                                           -- NB. Exists in CDA but not available in Workday
, cast(null as varchar)  as "Pay scale type"                                -- NB. Exists in CDA but not available in Workday
, cast(null as varchar)  as "Ar."                                           -- NB. Exists in CDA but not available in Workday
, cast(null as varchar)  as "Pay scale area"                                -- NB. Exists in CDA but not available in Workday
, cast(null as varchar)  as "PS group"                                      -- NB. Exists in CDA but not available in Workday
, cast(null as varchar)  as "Lv"                                            -- TBC contractual
, cast(null as varchar)  as "Annual salary"                                 -- TBC contractual
, cast(null as varchar)  as "Crrcy"                                         -- TBC contractual
, c.contracted_hours as "Wk.hrs."
, cast(null as varchar)  as "Customer-Specific Status"                      -- TBC
, h.holiday_zone_id as "PSubarea"
, h.holiday_zone_description as "Personnel Subarea"
, cast(null as varchar)  as "CT"                                            -- TBC
, cast(null as varchar)  as "Contract type"                                 -- TBC
, cast(null as varchar)  as "Cont Group"                                    -- TBC confidential
, cast(null as varchar)  as "Cont Var"
, cast(null as varchar)  as "Inc Scheme"
, to_varchar(p.position_start_date, 'DD.MM.YYYY') as "Pos Start Date"   -- NB. spec says Start Date as column name resulting in duplicate
, to_varchar(p.position_end_date, 'DD.MM.YYYY') as "Pos End Date"   -- NB. spec says End Date as column name resulting in duplicate
, cast(null as varchar)  as "Chngd on"                                      -- TBC
, cast(null as varchar)  as "Working week"
, to_varchar(p.position_end_date, 'DD.MM.YYYY') as "ContrctEnd"
, cast(null as varchar)  as "STI %"
, cast(null as varchar)  as "IRDNo"
, cast(null as varchar)  as "TaxCd"
, wl.location_id as "Building"
, wl.region as "Location"
, ma.colleague_id as "Superior Pers.no."                         -- NB. spec says Pers.no. as column name resulting in duplicate
, cast(null as varchar)  as "Personnel Number of Superior"                  -- TBC
, o.business_unit_id as "Business Unit"
, '' || '\t' as "Business Unit Text"                                    -- TBC  NB. Adding a TAB to mimic the behaviour of the original files.
from cte_final f
inner join {{ ref('dim_colleague') }} c on f.colleague_key = c.colleague_key
inner join {{ ref('dim_colleague') }} ma on f.manager_key = ma.colleague_key
inner join {{ ref('dim_colleague_confidential') }} cc on f.colleague_confidential_key = cc.colleague_confidential_key
inner join {{ ref('dim_position') }} p on f.position_key = p.position_key
inner join {{ ref('dim_job') }} j on f.position_job_key = j.job_key
inner join {{ ref('dim_colleague') }} m on f.manager_key = m.colleague_key
inner join {{ ref('dim_organisation_unit') }} o on f.organisation_unit_key = o.organisation_unit_key
inner join {{ ref('dim_location') }} wl on f.work_location_key = wl.location_key
inner join {{ ref('dim_holiday_zone') }} h on f.holiday_zone_key = h.holiday_zone_key
inner join {{ ref('dim_cost_center') }} cst on f.cost_center_key = cst.cost_center_key
inner join {{ ref('dim_employee_type') }} et on f.employee_type_key = et.employee_type_key
inner join {{ ref('dim_time_type') }} tt on f.time_type_key = tt.time_type_key
where coalesce(f.deleted, false) = false 
and c.colleague_id <> -1