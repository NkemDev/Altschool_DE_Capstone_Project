{{ config(
    materialized='table'
) }}

WITH source AS (
    SELECT
        p.product_category_name,
        ROUND(SUM(o.price), 2) AS total_price  
    FROM 
        {{ ref('stg_products') }} p
    JOIN 
        {{ ref('stg_order_items') }} o 
    ON 
        p.product_id = o.product_id
    JOIN 
        {{ ref('stg_orders') }} oo 
    ON
        oo.order_id = o.order_id
    WHERE 
        oo.order_status = 'delivered'  
    AND p.product_category_name IS NOT NULL  
    AND o.price IS NOT NULL  
    GROUP BY 
        p.product_category_name
    ORDER BY
        total_price DESC
)

SELECT 
    * 
FROM 
    source
ORDER BY
        total_price DESC