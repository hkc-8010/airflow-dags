{{ config(
  tags = ["main_processing"]
) }}

with cte_colleague as (
    select cast(case when left(to_varchar(c.colleague_id),2) IN('22','40') then right(repeat('0', 6) || substring(to_varchar(c.colleague_id),3),6) else to_varchar(c.colleague_id) end as varchar) as employee_nbr
    , cast(c.colleague_id as integer) as sap_employee_nbr
    , c.status as employment_status
    , c.full_name as employee_name
    , cast(p.position_id as integer) as position_nbr
    , p.position_description as position_text
    , j.job_id as job_nbr
    , j.job_description as job_text
    , cast(p.banker_flag as integer) as banker_flag    
    , cast(null as integer) as banker_level_nbr         -- TBC
    , cast(null as varchar) as banker_level_text        -- TBC
    , cast(null as varchar) as banker_role_type         -- TBC
    , p.people_leader_flag
    , cast(et.employee_type_id as string) as employee_type_code       
    , cast(et.employee_type_description as string) as employee_type_text       
    , cast(case when left(to_varchar(m.colleague_id),2) IN('22','40') then right(repeat('0', 6) || substring(to_varchar(m.colleague_id),3),6) else to_varchar(m.colleague_id) end as varchar) as people_leader_nbr
    , cast(m.colleague_id as integer) as people_leader_sap__nbr
    , m.full_name as people_leader_name
    , cast(o.organisation_unit_id as int) as organisational_unit_nbr
    , o.organisation_unit_description as organisational_unit_name
    , cast(o.business_unit_id as integer) as business_unit_nbr
    , o.business_unit_description as business_unit_name
    , cast(cc.cost_center_id as string) as cost_centre_code          
    , cast(cc.cost_center_description as string) as cost_centre_name    
    , wl.location_id as location_code
    , c.email_address as email
    , f.event_time
    , f.date_key
    , f.time_key
    , c.deleted
    from {{ ref('fact_colleague') }} f
    inner join {{ ref('dim_colleague') }} c on f.colleague_key = c.colleague_key
    inner join {{ ref('dim_position') }} p on f.position_key = p.position_key
    inner join {{ ref('dim_job') }} j on f.position_job_key = j.job_key
    inner join {{ ref('dim_colleague') }} m on f.manager_key = m.colleague_key
    inner join {{ ref('dim_organisation_unit') }} o on f.organisation_unit_key = o.organisation_unit_key
    inner join {{ ref('dim_location') }} wl on f.work_location_key = wl.location_key
    inner join {{ ref('dim_date') }} d on d.date_key = f.date_key
    inner join {{ ref('dim_time') }} t on t.time_key = f.time_key
    inner join {{ ref('dim_employee_type') }} et on f.employee_type_key = et.employee_type_key
    inner join {{ ref('dim_cost_center') }} cc on f.cost_center_key = cc.cost_center_key
    qualify row_number() over(partition by c.colleague_id, d.date order by t.time_of_day desc) = 1

), cte_final as (
    select *
    from cte_colleague
    qualify row_number() over(partition by employee_nbr order by event_time desc) = 1

)
select employee_nbr
    , sap_employee_nbr
    , employment_status
    , employee_name
    , position_nbr
    , position_text
    , job_nbr
    , job_text
    , banker_flag              
    , banker_level_nbr         
    , banker_level_text        
    , banker_role_type         
    , people_leader_flag
    , employee_type_code        
    , employee_type_text        
    , people_leader_nbr
    , people_leader_sap__nbr
    , people_leader_name
    , organisational_unit_nbr
    , organisational_unit_name
    , business_unit_nbr
    , business_unit_name
    , cost_centre_code          
    , cost_centre_name          
    , location_code
    , email
from cte_final
where coalesce(deleted, false) = false 
--and sap_employee_nbr = '40010205'

