{{  config(
    materialized='table',
    unique_key='id'
)}}

with source as (
    SELECT *
    FROM {{ source('staging', 'online_sales_data') }}
)

SELECT
    id,
    invoice_no,
    stock_code,
    description,
    quantity,
    invoice_date,
    unit_price,
    customer_id,
    country,
    discount,
    payment_method,
    shipping_cost,
    category,
    sales_channel,
    return_status,
    shipment_provider,
    warehouse_location,
    order_priority,

    -- Total Sales --
    ROUND((quantity * unit_price - COALESCE(discount, 0))::numeric, 2) AS total_sales,
    -- Total Revenue --
    ROUND(((quantity * unit_price - COALESCE(discount, 0)) + COALESCE(shipping_cost, 0))::numeric, 2) AS total_revenue,
    -- Case condition fields --
    CASE WHEN lower(return_status) = 'returned' THEN 1 ELSE 0 END AS is_returned,
    -- Extract DATE --
    EXTRACT(year from invoice_date) AS invoice_year,
    EXTRACT(month from invoice_date) AS invoice_month,
    EXTRACT(quarter from invoice_date) AS invoice_quarter

FROM source
WHERE quantity > 0
    AND unit_price > 0