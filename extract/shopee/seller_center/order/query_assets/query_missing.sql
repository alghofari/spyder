WITH sc_data AS (

SELECT
  'Order' AS data_type,
  'Shopee' AS marketplace,
  folder AS official_store,
  order_number AS customer_ref
FROM
  `sirclo-prod.bronze_seller_center_commerce.shopee_order`
WHERE
  create_delivery_time >= '{start_date} 00:00:00'
  AND create_delivery_time <= '{end_date} 23:59:59'

),

int_data AS (
SELECT
  DATE(order_tstamp) AS order_date,
  customer_reference AS customer_ref,
  official_store,
  order_status,
  marketplace
FROM
  silver_oms.detail_transaction_oms
WHERE 
  order_tstamp >= '{start_date} 00:00:00'
  AND order_tstamp <= '{end_date} 23:59:59'
  AND NOT REGEXP_CONTAINS(LOWER(official_store), 'agen toko|mitra toko')
  AND marketplace IN ('Shopee')
QUALIFY
  ROW_NUMBER() OVER(PARTITION BY customer_reference, marketplace ORDER BY update_tstamp DESC) = 1
),

combine_data AS (
SELECT 
  int.*,
  1 AS internal_order,
  IF(sc_order.customer_ref IS NOT NULL, 1, 0) AS scrap_order,
FROM
  int_data int
LEFT JOIN
  sc_data sc_order ON int.customer_ref = sc_order.customer_ref AND int.marketplace = sc_order.marketplace AND sc_order.data_type = 'Order'
),

aggregate AS (
SELECT
  *,
  CASE
    WHEN data_type = 'total_scrapped_order' THEN 'Sales'
    ELSE 'Other'
  END AS transaction_type,
  ROUND((100.0 - (SAFE_DIVIDE(count_order_type, total_internal_order) * 100.0)), 2) AS percentage_order_not_scrapped
FROM (
  SELECT
    marketplace,
    official_store,
    order_date,
    SUM(internal_order) AS total_internal_order,
    SUM(scrap_order) AS total_scrapped_order,
  FROM
    combine_data
  GROUP BY
    1, 2, 3
  )
UNPIVOT(count_order_type FOR data_type IN (total_scrapped_order))
)

SELECT
  official_store, array_agg(order_date) as order_date 
FROM
  aggregate
  where marketplace = 'Shopee' and transaction_type = 'Sales' and percentage_order_not_scrapped != 0.0 and official_store = '{official_store_name}' group by 1 