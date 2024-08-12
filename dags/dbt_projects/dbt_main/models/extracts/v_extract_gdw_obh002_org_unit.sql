
with colleague_position as
(
    select  pd.position_key ,  count(1) as  job_share_indicator
    from (
            select * from 
            (
                        select * 
                        from  {{ ref('fact_colleague') }}  fc
                        inner join  {{ ref('dim_colleague') }} dc on dc.colleague_key = fc.colleague_key
                        inner join {{ ref('dim_date')}}  d on d.date_key = fc.date_key
                        inner join {{ ref('dim_time')}}  t on t.time_key = fc.time_key
                        where status = 'ACTIVE'
                        qualify row_number() over(partition by colleague_id, d.date order by t.time_of_day desc) = 1
            ) as x
            qualify row_number() over(partition by colleague_id order by event_time desc) = 1       -- most recent day ;
    ) as latest
    inner join  {{ref('dim_position')}}  pd on pd.position_key = latest.position_key and pd.position_id != -1 
    group by pd.position_key

)
, cte_emppos as (
    select x.*
    from (select pd.position_id,
                pd.position_description,
                pd.position_end_date ,
                case when  pd.position_end_date is null or position_end_date < current_timestamp() then 
                coalesce(to_char(pd.position_end_date, 'YYYYMMDD'),'' ) else  '00010101'   end as date_of_status_change,  
                pd.position_start_date ,
                pd.customer_facing_role_flag,
                case when cp.job_share_indicator > 1 then 'Y' else  'N' end as job_share_indicator  , 
                l.location_id,
                l.city,
                l.country_id,
                l.address_line1,
                l.address_line2,
                l.address_line3,
                l.postal_code,
                cc.cost_center_id ,
                j.job_id,
                p.event_time, 
                pd.deleted 
            from {{ ref('fact_position') }} p
            inner join {{ref('dim_position')}} pd on pd.position_key = p.position_key
            left join colleague_position cp on cp.position_key = p.position_key
            inner join {{ref('dim_location')}} l on l.location_key = p.location_key
            inner join {{ref('dim_cost_center')}} cc on cc.cost_center_key = p.cost_center_key
            inner join {{ref('dim_job')}} j on j.job_key = p.job_key
            inner join {{ ref('dim_date') }} d on d.date_key = p.date_key
            inner join {{ ref('dim_time') }} t on t.time_key = p.time_key
            where pd.position_id != -1
            qualify row_number() over(partition by pd.position_id, d.date order by t.time_of_day desc) = 1      -- most recent fact for each day
        ) x
    qualify row_number() over(partition by position_id order by event_time desc) = 1       -- most recent day      

),
cte_hrorg as (
    select x.*
    from (select do.organisation_unit_id,
                do.organisation_unit_description,
                l.location_id,
                l.city,
                l.country_id,
                l.address_line1,
                l.address_line2,
                l.address_line3,
                l.postal_code,
                cc.cost_center_id ,
                o.event_time, 
                do.deleted 
            from {{ ref('fact_organisation_unit') }} o
            inner join {{ref('dim_organisation_unit')}} do on do.organisation_unit_key = o.organisation_unit_key
            inner join {{ref('dim_location')}} l on l.location_key = o.location_key
            inner join {{ref('dim_cost_center')}} cc on cc.cost_center_key = o.cost_center_key
            inner join {{ ref('dim_date') }} d on d.date_key = o.date_key
            inner join {{ ref('dim_time') }} t on t.time_key = o.time_key
            where do.organisation_unit_id != -1
            qualify row_number() over(partition by do.organisation_unit_id, d.date order by t.time_of_day desc) = 1      -- most recent fact for each day
        ) x
    qualify row_number() over(partition by organisation_unit_id order by event_time desc) = 1    
),

cte_detail as (
    select '1' as record_type_code
    , 'F' as record_action
    , left('BNZ' || repeat(' ', 10), 10) as financial_institution_id
    , left('SAP' || repeat(' ', 10), 10) as source_application_code
    , left(cast(x.position_local_code as varchar) || repeat(' ', 10), 10) as position_local_code
    , repeat(' ', 20) as filler
    , repeat('0', 12) as global_id
    , case when  x.date_of_status_change <> '00010101'  then left('VACANT' || repeat(' ', 6), 6)  else left('ACTIVE' || repeat(' ', 6), 6) end as position_status
    , left(coalesce(x.date_of_status_change,'') || repeat(' ', 8), 8) as date_of_status_change  
    , left(coalesce(x.city,'') || repeat(' ', 40), 40) as city
    , left(coalesce(x.country_id,'') || repeat(' ', 6), 6) as country
    , '+00000000000+00000000000' as constant                                            
    , left(coalesce(x.position_title,'') || repeat(' ', 100), 100) as position_title
    , '+00000000000+00000000000' as constant1                                            
    , left('UNDEF' || repeat(' ', 6), 6) as constant2                                            
    , left(coalesce(x.address_line1,'') || repeat(' ', 60), 60) as address_line_1
    , left(coalesce(x.address_line2,'') || repeat(' ', 60), 60) as address_line_2
    , left(coalesce(x.address_line3,'') || repeat(' ', 60), 60) as address_line_3
    , repeat(' ', 60 ) as address_line_4
    , left(coalesce(x.postal_code,'') || repeat(' ', 20), 20) as postal_code
    , repeat(' ', 6) as state_region_code
    , repeat(' ', 76) as filler2
    , left(coalesce(x.code_id, '') || repeat(' ', 6), 6) as code_id                                                    -- TBC 
    , repeat(' ', 40) as filler3
    , left(coalesce(x.cost_center_id,'') || repeat(' ', 6), 6) as cost_centre_code
    , repeat(' ', 6) as job_code --tbc
    , left(coalesce(to_char(x.position_start_date, 'YYYYMMDD'),'') || repeat(' ', 8), 8)  position_start_date
    , left(coalesce(to_char(x.position_end_date, 'YYYYMMDD'),'') || repeat(' ', 8), 8)  position_end_date
    , repeat(' ', 6)   organisation_group_code
    , left(coalesce(x.location_id,'') || repeat(' ', 30), 30) as dx_number
    , repeat(' ', 6) as position_employee_group 
    , repeat(' ', 6) location_code
    , repeat(' ', 6) location_type
    , '+' mrs_sign
    , repeat('0', 9) mrs
  , case when x.customer_facing_role_flag = true then left('Y' || repeat(' ', 6), 6) 
         when x.customer_facing_role_flag = false then left( 'N' || repeat(' ',6), 6) 
        else  repeat(' ', 6) end as customer_facing_indicator
    , left(coalesce(x.job_share_indicator,'') || repeat(' ', 6), 6)  job_share_indicator 
    , left(coalesce(x.job_id,'') || repeat(' ', 8), 8)  job_code1  

    from (
        select  position_id as position_local_code,  date_of_status_change , city , country_id , position_description as position_title,
                address_line1, address_line2, address_line3 , postal_code , cost_center_id , position_start_date,  position_end_date, location_id,
                customer_facing_role_flag,  job_share_indicator, job_id, event_time, deleted ,  'EMPPOS' as code_id
            from cte_emppos
            where coalesce(deleted, false) = false  
        union
        select  organisation_unit_id as position_local_code,  '00010101'  as date_of_status_change, city , country_id , organisation_unit_description as position_title, 
                address_line1, address_line2, address_line3 , postal_code, cost_center_id , '19000101' position_start_date,  '29991231' as position_end_date, location_id,
                Null as customer_facing_role_flag , ' ' as  job_share_indicator ,' ' as job_id , event_time, deleted,  'HRORG' as code_id
            from cte_hrorg
            where coalesce(deleted, false) = false  

    )as x 
)   
, cte_header as (
    select '0' as record_type_code
    , to_varchar(current_timestamp(), 'YYYYMMDDHH24MISS') as header_time_stamp
    , to_varchar(current_timestamp(), 'YYYYMMDD') as processing_date
    , 'BNZ' as financial_institution_id
    , 'HR' as data_source_system_code
    , left('obh002' || repeat(' ', 8), 8) as program_name                       -- input
    , left('Org Unit File' || repeat(' ', 40), 40) as file_description      -- input
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
    , left('obh002' || repeat(' ', 8), 8) as program_name                       -- input
    , left('Org Unit File' || repeat(' ', 40), 40) as file_description      -- input
    , '+' as sign1
    , '000000001' as file_identifier_code
    , '+' as sign2
    , right(repeat('0', 9) || to_varchar((select count(*) from cte_detail)), 9)  as record_count

), cte_union as (
    select record_type_code || header_time_stamp || processing_date || financial_institution_id || data_source_system_code || program_name || file_description || sign1 || file_identifier_code || sign2 || record_count as extract_column
    from cte_header
    union all
    select record_type_code || record_action || financial_institution_id || source_application_code || position_local_code || filler || global_id || position_status || date_of_status_change || city || country || constant || position_title || constant1 || constant2 || address_line_1 || address_line_2 || address_line_3 || address_line_4 || postal_code || state_region_code || filler2 || code_id || filler3 || cost_centre_code || job_code || position_start_date || position_end_date || organisation_group_code || dx_number || position_employee_group || location_code || location_type || mrs_sign || mrs || customer_facing_indicator || job_share_indicator || job_code1  as extract_column
    from cte_detail
    union all
    select record_type_code || trailer_time_stamp || processing_date || financial_institution_id || data_source_system_code || program_name || file_description || sign1 || file_identifier_code || sign2 || record_count as extract_column
    from cte_trailer
)
select *
from cte_union
order by 1