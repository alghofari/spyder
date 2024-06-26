INSERT INTO `sirclo-prod.bronze_seller_center_commerce.tiktok_data_overview` (
  time,
  revenue,
  refunds,
  unit_sales,
  buyers,
  product_views,
  visitors,
  conversion_rate,
  orders,
  sku_orders,
  store_name,
  load_timestamp
)
SELECT
  date,
  gmv,
  refunds,
  items_sold,
  buyers,
  page_views,
  visitors,
  conversion_rate,
  orders,
  sku_orders,
  store_name,
  load_timestamp
FROM `{temp_table_name}`