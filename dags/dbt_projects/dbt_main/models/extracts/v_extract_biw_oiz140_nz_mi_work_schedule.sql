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
, c.colleague_id as "Pers.no."
, c.full_name as "Name"
, cast(null as varchar) as "Date"
, cast(null as varchar) as "Day"
, cast(null as varchar) as "DWS"
, cast(null as varchar) as "DV"
, cast(null as varchar) as "Daily WS text"
, cast(null as varchar) as "Va"
, cast(null as varchar) as "Text"
, cast(null as varchar) as "Grp"
, cast(null as varchar) as "Start"
, cast(null as varchar) as "End"
, cast(null as varchar) as "PlHrs"
, cast(null as varchar) as "HCl"
, cast(null as varchar) as "DT"
, cast(null as varchar) as "DT text"
, cast(null as varchar) as "Personal WS"
, cast(null as varchar) as "Description"
, cast(null as varchar) as "HCr"
, cast(null as varchar) as "Text1"
, '' || '\t' as "Wk. time"                                      -- NB. Adding a TAB to mimic the behaviour of the original files.
from cte_fact_second_pass f
inner join {{ ref('dim_colleague') }} c on f.colleague_key = c.colleague_key
where coalesce(f.deleted, false) = false  
and c.colleague_id <> -1