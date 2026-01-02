{{ 
    config(
        materialized='table',
        schema='mart'
    )
}}

with calendar_dates as (
    select
        dateadd(day, seq4(), '2015-01-01') as date_day
    from table(generator(rowcount => 3653))  -- ~10 years of daily dates
)

select
    row_number() over (order by date_day) as dim_date_key,
    date_day as date_value,
    year(date_day) as year_value,
    month(date_day) as month_value,
    to_char(date_day, 'Month') as month_name,
    day(date_day) as day_of_month,
    to_char(date_day, 'DY') as day_of_week,
    dayofweekiso(date_day) as day_num_of_week,
    to_char(date_day, 'YYYY-MM') as year_month,
    case when extract(dow from date_day) in (6, 7) then 'Y' else 'N' end as is_weekend,
    current_timestamp as load_timestamp
from calendar_dates
order by date_value