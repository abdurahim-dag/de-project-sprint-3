alter table mart.f_sales add column if not exists start_date date;
alter table mart.f_sales add column if not exists end_date date;

alter table staging.user_order_log add column if not exists status varchar(50);
alter table mart.f_sales add column if not exists status varchar(50);

create or replace view mart.f_customer_retention
            (
             period_name,
             period_id,
             item_id,
             new_customers_count,
             new_customers_revenue,
             returning_customers_count,
             returning_customers_revenue,
             refunded_customer_count,
             customers_refunded
                ) as
with summary as (select
                             d.week || '.' || d.month_actual as period_id,
                             d.date_actual,
                             s.date_id,
                             s.item_id,
                             s.customer_id,
                             s.status,
                             s.payment_amount,
                             s.quantity
                 from mart.f_sales s
                          left join mart.d_calendar as d on s.date_id = d.date_id),
     customers as (select period_id,
                          item_id,
                          count(*)            as count_customer,
                          sum(payment_amount) as payment_customer
                   from summary s2
                   group by period_id, item_id),
     refunded_customer as (select period_id,
                                  item_id,
                                  count(*)      as count_customer,
                                  sum(quantity) as quantity_customer
                           from summary s2
                           where s2.status = 'refunded'
                           group by customer_id, period_id, item_id),
     summary_ids as (select distinct 'weekly' as period_name, period_id, item_id
                     from summary
     )

select s.period_name,
       s.period_id,
       s.item_id,
       new_customers.new_customers_count,
       new_customers.new_customers_revenue,
       returning_customers.returning_customers_count,
       returning_customers.returning_customers_revenue,
       refunded_customer.refunded_customer_count,
       refunded_customer.customers_refunded
from summary_ids s
         left join
     (select c.period_id,
             c.item_id,
             sum(c.count_customer)   new_customers_count,
             sum(c.payment_customer) new_customers_revenue
      from customers c
      where c.count_customer = 1
      group by c.period_id, c.item_id) new_customers
     on s.period_id = new_customers.period_id and s.item_id = new_customers.item_id
         left join
     (select c.period_id,
             c.item_id,
             sum(c.count_customer)   returning_customers_count,
             sum(c.payment_customer) returning_customers_revenue
      from customers c
      where c.count_customer > 1
      group by c.period_id, c.item_id) returning_customers
     on s.period_id = returning_customers.period_id and s.item_id = returning_customers.item_id
         left join
     (select c.period_id,
             c.item_id,
             sum(c.count_customer)    refunded_customer_count,
             sum(c.quantity_customer) customers_refunded
      from refunded_customer c
      group by c.period_id, c.item_id) refunded_customer
     on s.period_id = refunded_customer.period_id and s.item_id = refunded_customer.item_id


