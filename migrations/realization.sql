alter table mart.f_sales add start_date date;
alter table mart.f_sales add end_date date;

alter table staging.user_order_log add column if not exists status varchar(50);
alter table mart.f_sales add column if not exists status varchar(50);

create table mart.f_customer_retention
(
    id serial primary key,
    new_customers_count bigint not null,
    returning_customers_count bigint not null,
    refunded_customer_count bigint not null,
    period_name varchar(15),
    period_id        integer not null
        references mart.d_calendar,
    item_id        integer not null
        references mart.d_item (item_id),
    new_customers_revenue numeric(10, 2),
    returning_customers_revenue numeric(10, 2),
    customers_refunded       bigint,
    start_date date,
    end_date date
);

create index f_cr1
    on mart.f_customer_retention (period_id);

create index f_cr2
    on mart.f_customer_retention (item_id);



