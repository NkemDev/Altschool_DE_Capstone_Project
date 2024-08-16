with source as (
    select
    product_category_name_english,
    product_category_name
    
    from {{source('ecommerce','product_category_name_translation')}}
 )
 select * from source