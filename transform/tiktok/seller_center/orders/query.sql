MERGE sirclo-prod.bronze_seller_center_commerce.tiktok_order AS target
USING {temp_table_name} AS source
ON target.order_id = source.order_id
   AND target.sku_id = source.sku_id
   AND target.store_name = source.store_name
WHEN MATCHED THEN
  UPDATE SET
    target.order_status = source.order_status,
    target.order_substatus = source.order_substatus,
    target.cancelation_return_type = source.cancelation_return_type,
    target.normal_or_pre_order = source.normal_or_pre_order,
    target.seller_sku = source.seller_sku,
    target.product_name = source.product_name,
    target.variation = source.variation,
    target.quantity = source.quantity,
    target.sku_quantity_of_return = source.sku_quantity_of_return,
    target.sku_unit_original_price = source.sku_unit_original_price,
    target.sku_subtotal_before_discount = source.sku_subtotal_before_discount,
    target.sku_platform_discount = source.sku_platform_discount,
    target.sku_seller_discount = source.sku_seller_discount,
    target.sku_subtotal_after_discount = source.sku_subtotal_after_discount,
    target.shipping_fee_after_discount = source.shipping_fee_after_discount,
    target.original_shipping_fee = source.original_shipping_fee,
    target.shipping_fee_seller_discount = source.shipping_fee_seller_discount,
    target.shipping_fee_platform_discount = source.shipping_fee_platform_discount,
    target.taxes = source.taxes,
    target.order_amount = source.order_amount,
    target.order_refund_amount = source.order_refund_amount,
    target.created_time = source.created_time,
    target.paid_time = source.paid_time,
    target.rts_time = source.rts_time,
    target.shipped_time = source.shipped_time,
    target.delivered_time = source.delivered_time,
    target.cancelled_time = source.cancelled_time,
    target.cancel_by = source.cancel_by,
    target.cancel_reason = source.cancel_reason,
    target.fulfillment_type = source.fulfillment_type,
    target.warehouse_name = source.warehouse_name,
    target.tracking_id = source.tracking_id,
    target.delivery_option = source.delivery_option,
    target.shipping_provider_name = source.shipping_provider_name,
    target.buyer_message = source.buyer_message,
    target.buyer_username = source.buyer_username,
    target.recipient = source.recipient,
    target.phone = source.phone,
    target.zipcode = source.zipcode,
    target.country = source.country,
    target.province = source.province,
    target.regency_and_city = source.regency_and_city,
    target.districts = source.districts,
    target.villages = source.villages,
    target.detail_address = source.detail_address,
    target.additional_address_information = source.additional_address_information,
    target.payment_method = source.payment_method,
    target.weight_kg = source.weight_kg,
    target.product_category = source.product_category,
    target.package_id = source.package_id,
    target.seller_note = source.seller_note,
    target.checked_status = source.checked_status,
    target.checked_marked_by = source.checked_marked_by,
    target.store_name = source.store_name,
    target.load_timestamp = source.load_timestamp
WHEN NOT MATCHED THEN
  INSERT (order_id,
    order_status,
    order_substatus,
    cancelation_return_type,
    normal_or_pre_order,
    sku_id,
    seller_sku,
    product_name,
    variation,
    quantity,
    sku_quantity_of_return,
    sku_unit_original_price,
    sku_subtotal_before_discount,
    sku_platform_discount,
    sku_seller_discount,
    sku_subtotal_after_discount,
    shipping_fee_after_discount,
    original_shipping_fee,
    shipping_fee_seller_discount,
    shipping_fee_platform_discount,
    taxes,
    order_amount,
    order_refund_amount,
    created_time,
    paid_time,
    rts_time,
    shipped_time,
    delivered_time,
    cancelled_time,
    cancel_by,
    cancel_reason,
    fulfillment_type,
    warehouse_name,
    tracking_id,
    delivery_option,
    shipping_provider_name,
    buyer_message,
    buyer_username,
    recipient,
    phone,
    zipcode,
    country,
    province,
    regency_and_city,
    districts,
    villages,
    detail_address,
    additional_address_information,
    payment_method,
    weight_kg,
    product_category,
    package_id,
    seller_note,
    checked_status,
    checked_marked_by,
    store_name,
    load_timestamp
  )
  VALUES (order_id,
    order_status,
    order_substatus,
    cancelation_return_type,
    normal_or_pre_order,
    sku_id,
    seller_sku,
    product_name,
    variation,
    quantity,
    sku_quantity_of_return,
    sku_unit_original_price,
    sku_subtotal_before_discount,
    sku_platform_discount,
    sku_seller_discount,
    sku_subtotal_after_discount,
    shipping_fee_after_discount,
    original_shipping_fee,
    shipping_fee_seller_discount,
    shipping_fee_platform_discount,
    taxes,
    order_amount,
    order_refund_amount,
    created_time,
    paid_time,
    rts_time,
    shipped_time,
    delivered_time,
    cancelled_time,
    cancel_by,
    cancel_reason,
    fulfillment_type,
    warehouse_name,
    tracking_id,
    delivery_option,
    shipping_provider_name,
    buyer_message,
    buyer_username,
    recipient,
    phone,
    zipcode,
    country,
    province,
    regency_and_city,
    districts,
    villages,
    detail_address,
    additional_address_information,
    payment_method,
    weight_kg,
    product_category,
    package_id,
    seller_note,
    checked_status,
    checked_marked_by,
    store_name,
    load_timestamp
  )
