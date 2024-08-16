{{ config(materialized ='view')}}
with source_customer as (
    select 
    customer_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state

    from {{source('ecommerce','olist_customers')}}
 )
 select * from source_customer