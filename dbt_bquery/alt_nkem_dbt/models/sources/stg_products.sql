with source as (
    select
    product_id,
    product_category_name
    
    from {{source('ecommerce','olist_products')}}
 )
 select * from source