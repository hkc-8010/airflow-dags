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
select c.colleague_id as "Personnel no."
, to_varchar(c.hire_date, 'DD.MM.YYYY') as "Entry"
, 'Techn. date of entry' as "Date type"                      
, coalesce(to_varchar(c.hire_date, 'DD.MM.YYYY'), '') || '\t' as "Date"                         -- NB. Adding a TAB to mimic the behaviour of the original files.                     
from cte_final f
inner join {{ ref('dim_colleague') }} c on f.colleague_key = c.colleague_key
where coalesce(f.deleted, false) = false 
and c.colleague_id <> -1