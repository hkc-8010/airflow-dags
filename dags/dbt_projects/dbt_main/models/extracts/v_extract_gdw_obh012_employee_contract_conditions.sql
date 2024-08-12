
with cte_detail_base as (
    select '1' as record_type_code
    , 'F' as record_action
    , left('BNZ' || repeat(' ', 10), 10) as financial_institution_id
    , left('SAP' || repeat(' ', 10), 10) as source_application_code
    , left(cast(c.colleague_id as varchar) || coalesce(to_char(c.hire_date, 'YYYYMMDD'),'') || repeat(' ', 16), 16) as employee_local_code
    , repeat(' ', 14) as filler
    , repeat('0', 12) as global_id  
    , repeat(' ', 6) as contract_type               -- TBC confidential
    , right(repeat('0', 12) || '1', 12) as contract_counter
    , left(coalesce(to_char(c.hire_date, 'YYYYMMDD'),'') || repeat(' ', 8), 8) as start_date
    , '+' as sign1
    , repeat('0', 17) as fortnightly_hours          -- TBC confidential
    , c.colleague_id
    , f.event_time
    , c.deleted
    from {{ ref('fact_colleague') }} f
    inner join {{ ref('dim_colleague') }} c on f.colleague_key = c.colleague_key 
    inner join {{ ref('dim_colleague_confidential') }} cc on f.colleague_confidential_key = cc.colleague_confidential_key 
    inner join {{ ref('dim_time_type') }} tt on f.time_type_key = tt.time_type_key 
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
    , left('obh012' || repeat(' ', 8), 8) as program_name                       -- input
    , left('Employee Contract Conditions File' || repeat(' ', 40), 40) as file_description      -- input
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
    , left('obh012' || repeat(' ', 8), 8) as program_name                       -- input
    , left('Employee Contract Conditions File' || repeat(' ', 40), 40) as file_description      -- input
    , '+' as sign1
    , '000000001' as file_identifier_code
    , '+' as sign2
    , right(repeat('0', 9) || to_varchar((select count(*) from cte_detail where coalesce(deleted, false) = false)), 9)  as record_count

), cte_union as (
    select record_type_code || header_time_stamp || processing_date || financial_institution_id || data_source_system_code || program_name || file_description || sign1 || file_identifier_code || sign2 || record_count as extract_column
    from cte_header
    union all
    select record_type_code || record_action || financial_institution_id || source_application_code || employee_local_code || filler || global_id || contract_type || contract_counter || start_date || sign1 || fortnightly_hours  as extract_column
    from cte_detail
    where coalesce(deleted, false) = false  
    union all
    select record_type_code || trailer_time_stamp || processing_date || financial_institution_id || data_source_system_code || program_name || file_description || sign1 || file_identifier_code || sign2 || record_count as extract_column
    from cte_trailer
)
select *
from cte_union
order by 1