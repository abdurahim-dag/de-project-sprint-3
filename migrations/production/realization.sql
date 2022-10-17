create table mart.d_calendar
(
    date_id      integer not null
        primary key,
    date_actual  date,
    day_of_month integer,
    month_actual integer,
    month_name   varchar(50),
    year_actual  integer,
    "week"  integer
);

create index d_calendar1
    on mart.d_calendar (date_id);

create table mart.d_city
(
    id        serial primary key,
    city_id   integer unique,
    city_name varchar(50)
);

create index d_city1 on mart.d_city (city_id);

create table mart.d_customer
(
    id          serial
        primary key,
    customer_id integer not null
        unique,
    first_name  varchar(15),
    last_name   varchar(15),
    city_id     integer
);

create index d_cust1
    on mart.d_customer (customer_id);

create table mart.d_item
(
    id        serial
        primary key,
    item_id   integer not null
        unique,
    item_name varchar(50)
);

create unique index d_item1
    on mart.d_item (item_id);

create table mart.f_sales
(
    id             serial
        primary key,
    date_id        integer not null
        references mart.d_calendar,
    item_id        integer not null
        references mart.d_item (item_id),
    customer_id    integer not null
        references mart.d_customer (customer_id),
    city_id        integer not null,
    quantity       bigint,
    payment_amount numeric(10, 2)
);

create index f_ds1
    on mart.f_sales (date_id);

create index f_ds2
    on mart.f_sales (item_id);

create index f_ds3
    on mart.f_sales (customer_id);

create index f_ds4
    on mart.f_sales (city_id);
