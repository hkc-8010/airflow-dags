
with cte_detail_base as (
    select '1' as record_type_code
    , 'F' as record_action
    , left('BNZ' || repeat(' ', 10), 10) as financial_institution_id
    , left('SAP' || repeat(' ', 10), 10) as source_application_code
    , left(cast(p.position_id as varchar) || repeat(' ', 10), 10) as position_id
    , repeat(' ', 20) as filler
    , repeat('0', 12) as global_id
    , left('BNZ' || repeat(' ', 10), 10) as financial_institution_id1
    , left('SAP' || repeat(' ', 10), 10) as source_application_code1
    , left(cast(c.colleague_id as varchar) || coalesce(to_char(c.hire_date, 'YYYYMMDD'),'') || repeat(' ', 16), 16) as employee_local_code
    , repeat(' ', 14) as filler1
    , repeat('0', 12) as global_id1  
    , 'INCUM ' as position_code          
    , left(coalesce(to_char(p.position_start_date, 'YYYYMMDD'),'') || repeat(' ', 8), 8)   as start_date
    , repeat(' ', 6) as change_reason                                                   -- TBC validate the mapping table logic!!
    , left(coalesce(to_char(p.position_end_date, 'YYYYMMDD'),'') || repeat(' ', 8), 8)   as end_date
    , f.event_time
    , c.deleted
    from {{ ref('fact_position') }} f
    inner join {{ ref('dim_colleague') }} c on f.current_incumbent_key = c.colleague_key 
    inner join {{ ref('dim_position') }} p on f.position_key = p.position_key 
    inner join {{ ref('dim_date') }} d on d.date_key = f.date_key
    inner join {{ ref('dim_time') }} t on t.time_key = f.time_key
    inner join {{ ref('dim_action') }} ar on f.current_incumbent_action_key = ar.action_key
    --left join {{ ref('tbl_map_gdw_action_reason_code') }} ar on f.current_incumbent_action_key = ar.action_key
    where c.colleague_id <> -1
    and p.position_id <> -1
    qualify row_number() over(partition by p.position_id, d.date order by t.time_of_day desc) = 1      -- most recent fact for each day

), cte_detail as (
    select *
    from cte_detail_base
    qualify row_number() over(partition by position_id order by event_time desc) = 1       -- most recent day

), cte_header as (
    select '0' as record_type_code
    , to_varchar(current_timestamp(), 'YYYYMMDDHH24MISS') as header_time_stamp
    , to_varchar(current_timestamp(), 'YYYYMMDD') as processing_date
    , 'BNZ' as financial_institution_id
    , 'HR' as data_source_system_code
    , left('obh018' || repeat(' ', 8), 8) as program_name                       -- input
    , left('Employee org Unit Relationship File' || repeat(' ', 40), 40) as file_description      -- input
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
    , left('obh018' || repeat(' ', 8), 8) as program_name                       -- input
    , left('Employee org Unit Relationship File' || repeat(' ', 40), 40) as file_description      -- input
    , '+' as sign1
    , '000000001' as file_identifier_code
    , '+' as sign2
    , right(repeat('0', 9) || to_varchar((select count(*) from cte_detail where coalesce(deleted, false) = false)), 9)  as record_count

), cte_union as (
    select record_type_code || header_time_stamp || processing_date || financial_institution_id || data_source_system_code || program_name || file_description || sign1 || file_identifier_code || sign2 || record_count as extract_column
    from cte_header
    union all
    select record_type_code || record_action || financial_institution_id || source_application_code || position_id || filler || global_id || financial_institution_id1 || source_application_code1 || employee_local_code || filler1 || global_id1 || position_code || start_date || change_reason || end_date  as extract_column
    from cte_detail
    where coalesce(deleted, false) = false  
    union all
    select record_type_code || trailer_time_stamp || processing_date || financial_institution_id || data_source_system_code || program_name || file_description || sign1 || file_identifier_code || sign2 || record_count as extract_column
    from cte_trailer
)
select *
from cte_union
order by 1