
with cte_detail_base as (
    select '1' as record_type_code
    , 'F' as record_action
    , left('BNZ' || repeat(' ', 10), 10) as financial_institution_id
    , left('SAP' || repeat(' ', 10), 10) as source_application_code
    , left(cast(c.colleague_id as varchar) || coalesce(to_char(c.hire_date, 'YYYYMMDD'),'') || repeat(' ', 16), 16) as employee_local_code
    , repeat(' ', 14) as filler
    , repeat('0', 12) as global_id  
    , repeat('NV', 2) as pay_scale_type
    , repeat('NV', 2) as pay_scale_area
    , left('NV' || repeat(' ', 8), 8) as pay_scale_group
    , repeat(' ', 6) as contract_type               -- TBC confidential
    , left(cast(c.colleague_id as varchar) || coalesce(to_char(c.hire_date, 'YYYYMMDD'),'') || repeat(' ', 16), 16) as employee_number
    , repeat(' ', 14) as filler1
    , left(coalesce(to_char(c.hire_date, 'YYYYMMDD'),'') || repeat(' ', 8), 8) as start_date
    , case when lower(coalesce(c.status,'')) = 'active' then '29991231' else left(coalesce(to_char(c.organisation_assignment_end_date, 'YYYYMMDD'),'') || repeat(' ', 8), 8) end as end_date      
    , repeat(' ', 6) as termination_reason          -- TBC confidential
    , left('BNZ' || repeat(' ', 10), 10) as financial_institution_id1
    , left('SAP' || repeat(' ', 10), 10) as source_application_code1
    , left(cast(c.colleague_id as varchar) || coalesce(to_char(c.hire_date, 'YYYYMMDD'),'') || repeat(' ', 16), 16) as local_employee_id
    , repeat(' ', 14) as filler2
    , repeat('0', 12) as global_id1
    , left('BNZ' || repeat(' ', 10), 10) as financial_institution_id2
    , 'N' as customised_trp_indicator
    , repeat(' ', 6) as salary_pay_grade_level      -- TBC confidential
    , repeat(' ', 6) as salary_overtime_indicator   -- TBC confidential
    , left('BNZ' || repeat(' ', 10), 10) as financial_institution_id3
    , left('SAP' || repeat(' ', 10), 10) as source_application_code2
    , left('UNDEF' || repeat(' ', 30), 30) as local_agent_id
    , repeat('0', 12) as global_id2
    , repeat(' ', 3) as is_redeploy_flag            -- TBC
    , c.colleague_id
    , f.event_time
    , c.deleted
    from {{ ref('fact_colleague') }} f
    inner join {{ ref('dim_colleague') }} c on f.colleague_key = c.colleague_key 
    inner join {{ ref('dim_date') }} d on d.date_key = f.date_key
    inner join {{ ref('dim_time') }} t on t.time_key = f.time_key
    where c.colleague_id <> -1
    qualify row_number() over(partition by c.colleague_id, d.date order by t.time_of_day desc) = 1      -- most recent fact for each day

), cte_detail as (
    select *
    from cte_detail_base
    qualify row_number() over(partition by colleague_id order by event_time desc) = 1       -- most recent day

), cte_header as (
    select '0' as record_type_code
    , to_varchar(current_timestamp(), 'YYYYMMDDHH24MISS') as header_time_stamp
    , to_varchar(current_timestamp(), 'YYYYMMDD') as processing_date
    , 'BNZ' as financial_institution_id
    , 'HR' as data_source_system_code
    , left('obh010' || repeat(' ', 8), 8) as program_name                       -- input
    , left('Employee Contract File' || repeat(' ', 40), 40) as file_description      -- input
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
    , left('obh010' || repeat(' ', 8), 8) as program_name                       -- input
    , left('Employee Contract File' || repeat(' ', 40), 40) as file_description -- input
    , '+' as sign1
    , '000000001' as file_identifier_code
    , '+' as sign2
    , right(repeat('0', 9) || to_varchar((select count(*) from cte_detail where coalesce(deleted, false) = false)), 9)  as record_count

), cte_union as (
    select record_type_code || header_time_stamp || processing_date || financial_institution_id || data_source_system_code || program_name || file_description || sign1 || file_identifier_code || sign2 || record_count as extract_column
    from cte_header
    union all
    select record_type_code || record_action || financial_institution_id || source_application_code || employee_local_code || filler || global_id || pay_scale_type || pay_scale_area || pay_scale_group || contract_type || employee_number || filler1 || start_date || end_date || termination_reason || financial_institution_id1 || source_application_code1 || local_employee_id || filler2 || global_id1 || financial_institution_id2 || customised_trp_indicator || salary_pay_grade_level || salary_overtime_indicator || financial_institution_id3 || source_application_code2 || local_agent_id || global_id2 || is_redeploy_flag  as extract_column
    from cte_detail
    where coalesce(deleted, false) = false  
    union all
    select record_type_code || trailer_time_stamp || processing_date || financial_institution_id || data_source_system_code || program_name || file_description || sign1 || file_identifier_code || sign2 || record_count as extract_column
    from cte_trailer
)
select *
from cte_union
order by 1