with all_dates as (
    select distinct to_date(date_time::TEXT,'YYYY-MM-DD') as date_time from staging.user_activity_log
    union
    select distinct to_date(date_time::TEXT,'YYYY-MM-DD') as date_time from staging.user_order_log
    union
    select distinct to_date(date_id::TEXT,'YYYY-MM-DD') as date_time from staging.customer_research
    order by date_time
),
     all_dates_new as (
         select ad.* from all_dates ad
                              left join mart.d_calendar dc on dc.date_actual = ad.date_time
         where dc.date_actual is null
     )

INSERT INTO mart.d_calendar(date_id, date_actual, day_of_month, month_actual, month_name, year_actual, "week")
select
        coalesce((select max(date_id) from mart.d_calendar),0) +  ROW_NUMBER() OVER (
        ORDER BY date_time
        ) as date_id
     ,a.date_time as date_actual
     ,extract(day from a.date_time) as day_of_month
     ,extract(month from a.date_time) as month_actual
     ,TO_CHAR(a.date_time, 'Month') as month_name
     ,extract(year from a.date_time) as year_actual
     ,extract('day' from date_trunc('week', a.date_time) - date_trunc('week', date_trunc('month', a.date_time))) / 7 + 1 as "week"
from all_dates_new a;