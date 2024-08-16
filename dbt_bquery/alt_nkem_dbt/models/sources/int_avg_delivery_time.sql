{{ config(
    materialized='table'
) }}
with order_delivery as(
    select 
    order_id,
    order_status,
    order_purchase_timestamp,
    order_delivered_customer_date,
    TIMESTAMP_DIFF(order_delivered_customer_date,order_purchase_timestamp, minute) as delivery_time_minutes
    from {{(ref('stg_orders'))}}
    )
select 
    order_id,
    avg(delivery_time_minutes) as average_delivery_time
from order_delivery
group by order_id
