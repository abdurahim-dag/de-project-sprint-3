with all_customer as (
    select
        customer_id,
        first_name,
        last_name,
        max(city_id) as city_id
    from stage.user_order_log
    where customer_id not in (select customer_id from mart.d_customer)    
    group by customer_id,
        first_name,
        last_name
)
INSERT INTO mart.d_customer (customer_id, first_name, last_name, city_id)
select *
from all_customer;    