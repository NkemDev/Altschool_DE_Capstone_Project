with source_orders as (
    select
    order_id,
    customer_id,
    order_status,
    order_delivered_carrier_date,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_customer_date,
    order_estimated_delivery_date
    from {{source('ecommerce','olist_orders')}}
 )
 select * from source_orders
