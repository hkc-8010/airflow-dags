with cte_fact_first_pass as (
    select f.*, c.colleague_id, c.deleted
    from {{ ref('fact_colleague') }} f
    inner join {{ ref('dim_colleague') }} c on f.colleague_key = c.colleague_key
    inner join {{ ref('dim_date') }} d on d.date_key = f.date_key
    inner join {{ ref('dim_time') }} t on t.time_key = f.time_key
    qualify row_number() over(partition by c.colleague_id, d.date order by t.time_of_day desc) = 1      -- most recent fact for each day  

), cte_fact_second_pass as (
    select *
    from cte_fact_first_pass
    qualify row_number() over(partition by colleague_id order by event_time desc) = 1           -- most recent day  

)
select cast(null as varchar) as "Dummy"
, cst.cost_center_id  as "Cost ctr"                      
, cst.cost_center_Description as "CCT"    
, c.colleague_id as "Pers.no."
, c.full_name as "Name"
, cast(null as varchar) as "QTy."
, cast(null as varchar) as "Quota Text"
, cast(null as varchar) as "Accr Bal"
, cast(null as varchar) as "Entl Bal"
, cast(null as varchar) as "Total Bal"
, cast(null as varchar) as "Unit"
, '' || '\t' as "Liab Amt"                                                      -- NB. Adding a TAB to mimic the behaviour of the original files.
from cte_fact_second_pass f
inner join {{ ref('dim_colleague') }} c on f.colleague_key = c.colleague_key
inner join {{ ref('dim_cost_center') }} cst on f.cost_center_key = cst.cost_center_key
where coalesce(f.deleted, false) = false 
and c.colleague_id <> -1