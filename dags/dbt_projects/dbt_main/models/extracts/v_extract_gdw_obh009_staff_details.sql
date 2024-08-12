
with cte_detail_base as (
    select '1' as record_type_code
    , 'F' as record_action
    , left('BNZ' || repeat(' ', 10), 10) as financial_institution_id
    , left('SAP' || repeat(' ', 10), 10) as source_application_code
    , left(cast(c.colleague_id as varchar) || repeat(' ', 10), 10) as employee_local_code
    , repeat(' ', 20) as filler
    , repeat('0', 12) as global_id
    , left(cast(c.colleague_id as varchar) || repeat(' ', 10), 10) as original_employee_number
    , repeat(' ', 20) as filler1
    , repeat(' ', 15) as title                                                              -- TBC no title in CDA yet
    , left(coalesce(c.legal_first_name,'') || repeat(' ', 100), 100) as given_name
    , left(coalesce(c.initials,'') || repeat(' ', 6), 6) as initials
    , left(coalesce(c.legal_last_name,'') || repeat(' ', 100), 100) as surname
    , left(coalesce(c.preferred_first_name,'') || repeat(' ', 100), 100) as preferred_name
    , left(coalesce(to_char(cc.date_of_birth, 'YYYYMMDD'),'') || repeat(' ', 8), 8)  as birth_date          
    , left(coalesce(gm.sap_gender ,'') || repeat(' ', 6), 6)  as sex_code                                                                  
    , left(coalesce(to_char(c.hire_date, 'YYYYMMDD'),'') || repeat(' ', 8), 8)   as commencement_date
    , left(coalesce(to_char(c.organisation_assignment_end_date, 'YYYYMMDD'),'') || repeat(' ', 8), 8)   as end_date
    , left('BNZ' || repeat(' ', 10), 10) as financial_institution_id1                       -- TBC
    , left('BIS' || repeat(' ', 10), 10) as source_application_code1                        -- TBC
    , repeat(' ', 10) as customer_number                                                    -- TBC 
    , repeat(' ', 20) as filler2
    , repeat('0', 12) as global_id1
    , left('NOVAL' || repeat(' ', 6), 6) as mobility_code
    , left(coalesce(to_char(c.hire_date, 'YYYYMMDD'),'') || repeat(' ', 8), 8) as most_recent_hire_date
    , left('UNDEF' || repeat(' ', 6), 6) as preferred_specialised_group_code
    , left('UNDEF' || repeat(' ', 6), 6) as most_suitable_work_area
    , left('UNDEF' || repeat(' ', 6), 6) as most_suitable_next_position_code
    , f.event_time
    , c.deleted
    from {{ ref('fact_colleague') }} f
    inner join {{ ref('dim_colleague') }} c on f.colleague_key = c.colleague_key 
    inner join {{ ref('dim_colleague_confidential') }} cc on f.colleague_confidential_key = cc.colleague_confidential_key 
    inner join {{ ref('dim_date') }} d on d.date_key = f.date_key
    inner join {{ ref('dim_time') }} t on t.time_key = f.time_key
    left join {{ ref('tbl_map_gdw_gender') }} gm on cc.gender_identity = gm.wd_gender 
    where c.colleague_id <> -1
    qualify row_number() over(partition by c.colleague_id, d.date order by t.time_of_day desc) = 1      -- most recent fact for each day

), cte_detail as (
    select *
    from cte_detail_base
    qualify row_number() over(partition by original_employee_number order by event_time desc) = 1       -- most recent day

), cte_header as (
    select '0' as record_type_code
    , to_varchar(current_timestamp(), 'YYYYMMDDHH24MISS') as header_time_stamp
    , to_varchar(current_timestamp(), 'YYYYMMDD') as processing_date
    , 'BNZ' as financial_institution_id
    , 'HR' as data_source_system_code
    , left('obh009' || repeat(' ', 8), 8) as program_name                       -- input
    , left('Staff Member File' || repeat(' ', 40), 40) as file_description      -- input
    , '+' as sign1
    , '000000001' as file_identifier_code
    , '+' as sign2
    , '000000000' record_count

), cte_trailer as (
    select '9' as record_type_code
    , to_varchar(current_timestamp(), 'YYYYMMDDHH24MISS') as trailer_time_stamp
    , to_varchar(current_timestamp(), 'YYYYMMDD') as processing_date
    , 'BNZ' as financial_institution_id
    , 'HR' as data_source_system_code
    , left('obh009' || repeat(' ', 8), 8) as program_name                       -- input
    , left('Staff Member File' || repeat(' ', 40), 40) as file_description      -- input
    , '+' as sign1
    , '000000001' as file_identifier_code
    , '+' as sign2
    , right(repeat('0', 9) || to_varchar((select count(*) from cte_detail where coalesce(deleted, false) = false)), 9)  as record_count

), cte_union as (
    select record_type_code || header_time_stamp || processing_date || financial_institution_id || data_source_system_code || program_name || file_description || sign1 || file_identifier_code || sign2 || record_count as extract_column
    from cte_header
    union all
    select record_type_code || record_action || financial_institution_id || source_application_code || employee_local_code || filler || global_id || original_employee_number || filler1 || title || given_name || initials || surname || preferred_name || birth_date || sex_code || commencement_date || end_date || financial_institution_id1 || source_application_code1 || customer_number || filler2 || global_id1 || mobility_code || most_recent_hire_date || preferred_specialised_group_code || most_suitable_work_area || most_suitable_next_position_code as extract_column
    from cte_detail
    where coalesce(deleted, false) = false  
    union all
    select record_type_code || trailer_time_stamp || processing_date || financial_institution_id || data_source_system_code || program_name || file_description || sign1 || file_identifier_code || sign2 || record_count as extract_column
    from cte_trailer
)
select *
from cte_union
order by 1