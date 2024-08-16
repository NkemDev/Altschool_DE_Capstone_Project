{{ config(
    materialized='table'
) }}
WITH product_data AS (
    SELECT 
        p.product_category_name,
        SUM(o.price) AS total_price
    FROM 
        {{ ref('stg_products') }} p
    JOIN 
        {{ ref('stg_order_items') }} o 
    ON 
        p.product_id = o.product_id  -- Cast if necessary
    JOIN 
        {{ ref('stg_orders') }} oo 
    ON
        oo.order_id = o.order_id
    GROUP BY 
        p.product_category_name
)

SELECT * FROM product_data
