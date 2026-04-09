{{ config(materialized='table') }}

SELECT
    order_id,
    customer_id,
    order_status,
    record_status,
    CASE
        WHEN record_status = 'suspicious' THEN 1
        ELSE 0
    END AS is_suspicious
FROM {{ ref('orders_final') }}