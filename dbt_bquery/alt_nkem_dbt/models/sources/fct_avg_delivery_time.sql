{{ config(
    materialized='table'
) }}
with fct_order_delivery as(
    select 
    order_id,
    order_status,
    order_purchase_timestamp,
    order_delivered_customer_date,
    TIMESTAMP_DIFF(order_delivered_customer_date,order_purchase_timestamp, day) as delivery_time_minutes
    from {{(ref('stg_orders'))}}
    where order_status ='delivered'
    and TIMESTAMP_DIFF(order_delivered_customer_date,order_purchase_timestamp, day) is not null
    )
select 
    order_id,
    avg(delivery_time_minutes) as average_delivery_time
from fct_order_delivery
group by order_id
order by average_delivery_time desc