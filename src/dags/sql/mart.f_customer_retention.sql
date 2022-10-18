
with s1 as (select 'weekly'                        as period_name,
                   d.week || '.' || d.month_actual as period_id,
                   d.date_actual,
                   s.item_id,
                   s.customer_id,
                   s.status,
                   s.payment_amount,
                   s.quantity
            from mart.f_sales s
                     left join mart.d_calendar as d on s.date_id = d.date_id

),
s2 as (
select
   s1.customer_id, s1.period_id
   , s1.period_name
   , s1.item_id
   , s1.status
   , count(*) as count_status
   , sum(s1.payment_amount) as sum_payment_amount
   , sum(s1.quantity) as sum_quantity
    from s1
    group by period_id
   , s1.status
   , s1.period_name
   , s1.item_id
   , s1.customer_id
    ),
new_customers as (select
                      period_name, period_id, item_id,
                      sum(count_status) as count_customer,
                      sum(sum_payment_amount) as payment_customer

                        from s2
                        group by customer_id, period_name, period_id, item_id
                        having sum(count_status) = 1),
returning_customers as (select
                            period_name, period_id, item_id,
                            sum(count_status) as count_customer,
                            sum(sum_payment_amount) as payment_customer
                        from s2
                        group by customer_id, period_name, period_id, item_id
                        having sum(count_status) > 1),
refunded_customer as (
select
    period_name, period_id, item_id,
    sum(count_status) as count_customer,
       sum(sum_quantity) as quantity_customer
from s2 where s2.status='refunded'
group by customer_id, period_name, period_id, item_id )
select
    s.period_name, s.period_id, s.item_id,
    sum(nc.count_customer) as new_customers_count,
    sum(nc.payment_customer) as new_customers_revenue,
    sum(rc.count_customer) as returning_customers_count ,
    sum(rc.payment_customer) as returning_customers_revenue,

    sum(cr.count_customer) as refunded_customer_count ,
    sum(cr.quantity_customer) as customers_refunded

from s2 s
    left join new_customers nc on
        nc.period_name = s.period_name
        and nc.period_id = s.period_id
        and nc.item_id = s.item_id
    left join refunded_customer cr on
        cr.period_name = s.period_name
        and cr.period_id = s.period_id
        and cr.item_id = s.item_id
    left join returning_customers rc on
        rc.period_name = s.period_name
        and rc.period_id = s.period_id
        and rc.item_id = s.item_id
group by s.period_name, s.period_id, s.item_id

