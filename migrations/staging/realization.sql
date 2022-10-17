create schema staging;
create table staging.customer_research (
    id serial,
    date_id timestamp without time zone,
    category_id integer,
    geo_id integer,
    sales_qty integer,
    sales_amt numeric(14,2)
);
create table staging.user_activity_log (
    id serial,
    date_time timestamp without time zone,
    action_id bigint,
    customer_id bigint,
    quantity bigint
);
create table staging.user_order_log
(
    id             serial,
    date_time      timestamp without time zone,
    city_id        integer,
    city_name      varchar(100),
    customer_id    bigint,
    first_name     varchar(100),
    last_name      varchar(100),
    item_id        integer,
    item_name      varchar(100),
    quantity       bigint,
    payment_amount numeric(14, 2)
);