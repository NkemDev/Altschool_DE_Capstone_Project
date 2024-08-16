{{ config(
    materialized='table'
) }}
WITH source AS (
    SELECT
        order_id,
        DATE_DIFF(
            DATE(SAFE_CAST(order_delivered_customer_date AS TIMESTAMP)),
            DATE(SAFE_CAST(order_purchase_timestamp AS TIMESTAMP)),
            DAY
        ) AS delivery_time_days
    FROM {{ ref('stg_orders') }}
    
)SELECT
    order_id,
    AVG(delivery_time_days) AS avg_delivery_time_days
FROM source
group by order_id 
order by avg_delivery_time_days