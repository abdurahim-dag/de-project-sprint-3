delete from mart.f_sales
where start_date = '{{ds}}' and end_date = '{{ds}}';

insert into mart.f_sales (date_id, item_id, customer_id, city_id, quantity, payment_amount, status, start_date, end_date)
select dc.date_id, item_id, customer_id, city_id, quantity,
       case when uol.status = 'refunded' then -1 * payment_amount else payment_amount end as payment_amount,
       uol.status,
       '{{ds}}' as start_date, '{{ds}}' as end_date
from staging.user_order_log uol
         left join mart.d_calendar as dc on uol.date_time::Date = dc.date_actual;
