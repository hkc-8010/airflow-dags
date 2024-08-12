
{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='date_key',
        tags = ["pre_processing"]
    )
}}


with recursive calendardates as (
    /*select datefromparts(year(to_timestamp('2000-01-01')) , 1, 1) as calendar_date
    union all
    select dateadd(day, 1, calendar_date)
    from calendardates
    where calendar_date < dateadd(year, 50, datefromparts(year(to_timestamp('2000-01-01')), 1, 1)) - 1*/
    select to_date('1900-01-01') as calendar_date, 1 as unknown_member
    union
    select to_date(dateadd(day, seq4(), '1950-01-01')), 0
    from table(generator(rowcount=>40000))
)
select
    case when unknown_member = 1 then {{ var("unknown_hash") }} else md5(coalesce(cast(calendar_date as string),'')) end as date_key
    ,calendar_date as date
    ,year(calendar_date) as year
    ,month(calendar_date) as month
    ,monthname(calendar_date) as monthname
    ,day(calendar_date) as day
    ,dayname(calendar_date) as dayname
    ,quarter(calendar_date) as quarter
    ,week(calendar_date) as week

    ,dayofmonth(calendar_date) as dayofmonth
    ,dayofweek(calendar_date) as dayofweek
    ,dayofyear(calendar_date) as dayofyear

    ,last_day (calendar_date) as last_day_of_month
    ,last_day (calendar_date, 'quarter') as last_day_of_quarter
    ,last_day (calendar_date, 'year') as last_day_of_year
    ,last_day (calendar_date, 'week') as last_day_of_week

    ,weekofyear(calendar_date) as weekofyear
    ,yearofweek(calendar_date) as yearofweek
    ,yearofweekiso(calendar_date) as yearofweekiso
    ,weekiso(calendar_date) as weekiso
    ,dayofweekiso(calendar_date) as dayofweekiso

    ,ceil(dayofmonth(calendar_date)/7) as week_of_month

    ,case
        when quarter(calendar_date) = 1 then datefromparts(year(calendar_date), 1, 1)
        when quarter(calendar_date) = 2 then datefromparts(year(calendar_date), 4, 1)
        when quarter(calendar_date) = 3 then datefromparts(year(calendar_date), 7, 1)
    else 
        datefromparts(year(calendar_date), 10, 1)
    end as first_day_of_quarter
    ,datediff (day, first_day_of_quarter, date) + 1 as day_of_quarter
    ,datediff (day, first_day_of_quarter, last_day_of_quarter) + 1 as days_in_quarter
    ,ceil(day_of_quarter/7) as week_of_quarter

    , current_timestamp() as created_at

from calendardates  

{% if is_incremental() %}
        where  date > (select max(date) from {{ this }} )
{% endif %}
