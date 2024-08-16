{{ config(
    materialized='table'
) }}

with source as(
    select 
        count(o.order_id) as count_orders,
        c.customer_state
    from
        {{ref ('stg_orders')}}o
    JOIN
        {{ref ('stg_customers')}}c 
    on 
        o.customer_id =c.customer_id
    where o.order_status ='delivered'
    group by 
        c.customer_state

)
select * from source
order by count_orders desc