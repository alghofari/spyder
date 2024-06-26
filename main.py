import argparse
from datetime import date, datetime, timedelta

if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("--marketplace", type=str, required=True)
    arg_parser.add_argument("--job_type", type=str, required=True)
    arg_parser.add_argument("--page_type", type=str, required=True)

    arg_parser.add_argument("--start_page", type=int, required=False)
    arg_parser.add_argument("--end_page", type=int, required=False)
    arg_parser.add_argument("--category_name", type=str, required=False)
    arg_parser.add_argument("--bucket_name", type=str, required=False)
    arg_parser.add_argument("--base_path", type=str, required=False)
    arg_parser.add_argument("--base_path_exitm", type=str, required=False)
    arg_parser.add_argument("--base_path_itm", type=str, required=False)
    arg_parser.add_argument("--target_table", type=str, required=False)
    arg_parser.add_argument("--target_table_cat", type=str, required=False)
    arg_parser.add_argument("--target_table_itm", type=str, required=False)
    arg_parser.add_argument("--sort_by", type=str, required=False)
    arg_parser.add_argument("--short_slp", type=int, required=False)
    arg_parser.add_argument("--long_slp", type=int, required=False)
    arg_parser.add_argument("--get_prev_date", type=int, required=False)
    arg_parser.add_argument("--prev_date", type=str, required=False)
    arg_parser.add_argument("--start_date", type=str, required=False, default=datetime.strftime(datetime.now() - timedelta(days=6), '%Y-%m-%d'))
    arg_parser.add_argument("--end_date", type=str, required=False, default=datetime.strftime(datetime.now(), '%Y-%m-%d'))
    arg_parser.add_argument("--extract_type", type=str, required=False, default="by_element")
    arg_parser.add_argument("--min_sleep", type=int, required=False, default=60)
    arg_parser.add_argument("--max_sleep", type=int, required=False, default=60)
    arg_parser.add_argument("--schema_path", type=str, required=False)
    arg_parser.add_argument("--extract_query", type=str, required=False)
    arg_parser.add_argument("--instagram_username", type=str, required=False)
    arg_parser.add_argument("--instagram_password", type=str, required=False)
    arg_parser.add_argument("--topic", type=str, required=False)
    arg_parser.add_argument("--bootstrap_servers", type=str, required=False)
    arg_parser.add_argument("--auto_offset_reset", type=str, required=False)
    arg_parser.add_argument("--enable_auto_commit", type=bool, required=False)
    arg_parser.add_argument("--group_id", type=str, required=False)
    arg_parser.add_argument("--shop_name", type=str, required=False)
    arg_parser.add_argument("--page_part", type=str, required=False, default="all")
    arg_parser.add_argument("--os_key", type=str, required=False)
    arg_parser.add_argument("--proxy_use", type=str, required=False)
    arg_parser.add_argument("--past_data", type=str, required=False)
    arg_parser.add_argument("--store_name", type=str, required=False)
    arg_parser.add_argument("--condition", type=str, required=False)
    arg_parser.add_argument("--date_first", type=str, required=False)
    arg_parser.add_argument("--range_date", type=int, required=False)
    arg_parser.add_argument("--vpn_use", type=str, required=False)
    arg_parser.add_argument("--categories_csv", type=str, required=False)
    arg_parser.add_argument("--oxylab_use", type=str, required=False)
    arg_parser.add_argument("--profile_path", type=str, required=False)
    arg_parser.add_argument("--start_index", type=int, required=False)
    arg_parser.add_argument("--end_index", type=int, required=False)
    arg_parser.add_argument("--username", type=str, required=False)
    arg_parser.add_argument("--password", type=str, required=False)

    args = arg_parser.parse_args()

    run_date = date.today().strftime("%Y-%m-%d")
    now = datetime.now()

    if args.marketplace == "instagram":
        if args.job_type == "extract":
            if args.page_type == "merchant_acquisition":
                from extract.instagram import merchant_acquisition
                merchant_acquisition.run(args.instagram_username, args.instagram_password)
            if args.page_type == "ibusibuk":
                from extract.instagram import ibusibuk
                ibusibuk.main(args.instagram_username, args.instagram_password)

    if args.marketplace == "lazada":
        if args.job_type == "extract":
            if args.page_type == "search_item":
                from extract.lazada.search_item import lazada_item
                lazada_item.main(
                    args.start_page,
                    args.end_page,
                    args.category_name,
                    args.bucket_name,
                    args.base_path,
                    run_date,
                    now,
                    args.target_table,
                    args.schema_path,
                    args.vpn_use,
                    args.oxylab_use
                )

            if args.page_type == "seller_center":
                from extract.lazada.seller_center import main
                main.execute(args.store_name)
            
            if args.page_type == "lazmall": 
                from extract.lazada.lazada_mall import lazada_mall
                lazada_mall.main(
                    args.start_page,
                    args.end_page,
                    args.bucket_name,
                    args.base_path,
                    run_date,
                    now,
                    args.target_table,
                    args.schema_path,
                    args.oxylab_use,
                    args.vpn_use
                )

    if args.marketplace == "bukalapak":
        if args.job_type == "extract":
            if args.page_type == "search_item":
                from extract.bukalapak.search_item import bukalapak_item
                bukalapak_item.main(
                    args.start_page,
                    args.end_page,
                    args.bucket_name,
                    args.base_path,
                    run_date,
                    args.target_table,
                    args.schema_path,
                )

    if args.marketplace == "blibli":
        if args.job_type == "extract":
            if args.page_type == "search_item":
                from extract.blibli.search_item import search_item as blibli_search_item
                blibli_search_item.main(
                    args.start_page,
                    args.end_page,
                    args.bucket_name,
                    args.base_path,
                    run_date,
                    args.target_table,
                    args.schema_path,
                    args.vpn_use,
                    args.oxylab_use
                )

    if args.marketplace == "shopee":
        if args.job_type == "extract":
            if args.page_type == "category":
                from extract.shopee.shopee_bs import category_page as shopee_extract_category
                shopee_extract_category.main(
                    args.bucket_name,
                    args.base_path,
                    run_date,
                    args.sort_by
                )
            if args.page_type == "item":
                from extract.shopee.shopee_bs import item_page as shopee_extract_item
                shopee_extract_item.main(
                    args.category_name,
                    args.bucket_name,
                    args.base_path,
                    run_date,
                    args.short_slp,
                    args.long_slp
                )
            if args.page_type == "search_item":
                from extract.shopee.shopee_item import search_item as shopee_search_item
                shopee_search_item.main(
                    args.start_page,
                    args.end_page,
                    args.bucket_name,
                    args.base_path,
                    args.sort_by,
                    run_date,
                    args.target_table,
                    args.schema_path,
                    args.category_name,
                    args.bootstrap_servers,
                    args.topic
                )
            if args.page_type == "search_item_v2":
                from extract.shopee.shopee_item import search_item_v2 as shopee_search_item
                shopee_search_item.main(
                    args.bucket_name,
                    args.base_path,
                    args.sort_by,
                    run_date,
                    args.target_table,
                    args.schema_path
                )
            if args.page_type == "all_shop_info":
                from extract.shopee.shopee_mall import all_shop_info as shopee_all_shop
                shopee_all_shop.main(
                    args.base_path,
                    run_date,
                    args.target_table,
                    args.schema_path
                )
            if args.page_type == "shop_detail":
                from extract.shopee.shopee_mall import shop_detail as extract_shopee_shop_detail
                extract_shopee_shop_detail.main(
                    args.bucket_name,
                    args.base_path,
                    args.extract_query,
                    run_date
                )
                from transform.shopee.shopee_mall import shop_detail as transform_shopee_shop_detail
                transform_shopee_shop_detail.main(
                    args.bucket_name,
                    args.base_path,
                    args.target_table,
                    args.schema_path,
                    now,
                    run_date
                )

            if args.page_type == "shopee_mall_item":
                from extract.shopee.shopee_mall import mall_item as extract_mall_item
                extract_mall_item.main(
                    args.start_page,
                    args.end_page,
                    args.bucket_name,
                    args.base_path,
                    run_date,
                    args.target_table,
                    args.schema_path,
                    args.shop_name,
                    args.vpn_use,
                    args.oxylab_use
                )

            if args.page_type == "shopee_mall_review":
                from extract.shopee.shopee_mall import review_mall_item
                review_mall_item.main(
                    args.start_page,
                    args.end_page,
                    args.base_path,
                    args.bucket_name,
                    args.target_table,
                    args.schema_path
                )

            if args.page_type == "mall_detail":
                from extract.shopee.shopee_mall import mall_shop_detail as extract_mall_shop_detail
                extract_mall_shop_detail.main(
                    args.bucket_name,
                    args.base_path,
                    run_date,
                    args.target_table,
                    args.schema_path
                )

            if args.page_type == "seller_center":
                from extract.shopee.seller_center import main
                main.execute(
                    args.start_date,
                    args.end_date,
                    args.os_key
                )

            if args.page_type == "sub_account":
                from extract.shopee.sub_account import main
                main.execute(args.os_key, args.profile_path)

        if args.job_type == "DLQ":
            from extract.shopee.consumer import consumer as shopee_consumer
            shopee_consumer.main(
                args.topic,
                args.bootstrap_servers,
                args.auto_offset_reset,
                args.enable_auto_commit,
                args.group_id
            )
        if args.job_type == 'transform':
            if args.page_type == 'category':
                from transform.shopee.shopee_bs import read_html_category as shopee_transform_category
                shopee_transform_category.main(
                    args.category_name,
                    args.base_path,
                    args.bucket_name,
                    args.target_table,
                    run_date
                )
            if args.page_type == 'item':
                from transform.shopee.shopee_bs import read_html_item as shopee_transform_item
                shopee_transform_item.main(
                    args.category_name,
                    args.bucket_name,
                    args.base_path,
                    args.target_table,
                    run_date,
                    args.get_prev_date,
                    args.prev_date
                )
            if args.page_type == 'search_item':
                from transform.shopee.search_item import transform_item as shopee_transform_search_item
                shopee_transform_search_item.main(
                    args.category_name,
                    args.bucket_name,
                    args.base_path,
                    args.target_table,
                    now,
                    args.schema_path,
                    args.sort_by
                )
            if args.page_type == 'seller_center':
                from transform.shopee.seller_center import load_to_bq
                load_to_bq.load_to_bq(
                    args.start_date,
                    args.end_date,
                    args.os_key
                )
            if args.page_type == 'live_stream':
                from transform.shopee.seller_center.live_stream import load_live_stream_to_bq
                load_live_stream_to_bq.main(
                    args.os_key
                )
            if args.page_type == 'traffic':
                from transform.shopee.seller_center.traffic import load_traffic_to_bq
                load_traffic_to_bq.main(args.os_key)
            if args.page_type == 'store_performance':
                from transform.shopee.seller_center.store_performance import load_store_performance_to_bq
                load_store_performance_to_bq.main(args.os_key)
            if args.page_type == 'return_order':
                from transform.shopee.seller_center.return_order import load_return_order_to_bq
                load_return_order_to_bq.main(
                    args.start_date,
                    args.end_date,
                    args.os_key
                )
            if args.page_type == 'raw_fulfillment':
                from transform.shopee.seller_center.raw_fulfillment import load_raw_fulfillment_to_bq
                load_raw_fulfillment_to_bq.main(
                    args.start_date,
                    args.end_date,
                    args.os_key
                )

    if args.marketplace == "tokopedia":
        if args.job_type == "DLQ":
            from extract.tokopedia.consumer import consumer as tokopedia_consumer
            tokopedia_consumer.main(
                args.topic,
                args.bootstrap_servers,
                args.auto_offset_reset,
                args.enable_auto_commit,
                args.group_id
            )
        if args.job_type == "extract":
            if args.page_type == "category":
                from extract.tokopedia.category import category_page as tokopedia_extract_category
                tokopedia_extract_category.main(
                    args.start_page,
                    args.end_page,
                    args.base_path,
                    run_date,
                    args.schema_path,
                    args.target_table,
                    args.proxy_use,
                    args.vpn_use,
                    args.sort_by
                )
            if args.page_type == "item":
                from extract.tokopedia import item_page as tokopedia_extract_item
                tokopedia_extract_item.main(
                    args.category_name,
                    args.bucket_name,
                    args.base_path,
                    run_date
                )
            if args.page_type == "search_item":
                from extract.tokopedia.search_item import tokopedia_item as tokopedia_search_item
                tokopedia_search_item.main(
                    args.start_page,
                    args.end_page,
                    args.bucket_name,
                    args.base_path,
                    run_date,
                    now,
                    args.target_table,
                    args.schema_path,
                    args.categories_csv,
                    args.proxy_use,
                    args.past_data,
                    args.vpn_use
                )
            if args.page_type == "search_item_priority":
                from extract.tokopedia.search_item import tokopedia_item_priority as tokopedia_search_item
                tokopedia_search_item.main(
                    args.start_page,
                    args.end_page,
                    args.category_name,
                    args.bucket_name,
                    args.base_path,
                    run_date,
                    now,
                    args.target_table,
                    args.schema_path,
                    args.bootstrap_servers,
                    args.topic
                )
                # from transform.tokopedia.search_item import tokopedia_item_tfm as tokopedia_transform_search_item
                # tokopedia_transform_search_item.main(
                #     args.category_name,
                #     args.base_path,
                #     now,
                #     args.bucket_name,
                #     args.target_table,
                #     args.schema_path
                # )
            if args.page_type == "official_store":
                from extract.tokopedia.official_store import tokopedia_os
                tokopedia_os.main(
                    args.end_page,
                    args.bucket_name,
                    args.base_path,
                    run_date,
                    now,
                    args.target_table,
                    args.schema_path
                )
            if args.page_type == "official_store_item":
                from extract.tokopedia.official_store import tokopedia_os_item
                tokopedia_os_item.main(
                    args.bucket_name,
                    args.base_path,
                    run_date,
                    now,
                    args.target_table,
                    args.schema_path
                )
            if args.page_type == "seller_center":
                if args.extract_type == "by_api":
                    from extract.tokopedia.seller_center import main
                    main.execute(args.os_key,
                                 args.page_part,
                                 args.start_date,
                                 args.end_date)

        if args.job_type == 'transform':
            if args.page_type == 'category':
                from transform.tokopedia.category import transform_category_page as tokopedia_transform_category
                tokopedia_transform_category.main(
                    args.category_name,
                    args.base_path,
                    args.bucket_name,
                    args.target_table,
                    run_date
                )
            if args.page_type == 'item':
                from transform.tokopedia.category import transform_item_page as tokopedia_transform_item
                tokopedia_transform_item.main(
                    args.category_name,
                    args.base_path,
                    args.bucket_name,
                    args.target_table,
                    run_date
                )
            if args.page_type == 'seller_center':
                if args.condition == 'main':
                    from transform.tokopedia.seller_center.sales import transform_sales_page
                    transform_sales_page.main(args.os_key)

                if args.condition == 'backfill' :
                    from transform.tokopedia.seller_center.sales import backfill_seller_center_page
                    backfill_seller_center_page.main(args.os_key, args.date_first, args.range_date)

            if args.page_type == 'statistic':
                from transform.tokopedia.seller_center.statistic import transform_statistic_page
                transform_statistic_page.main(args.os_key, args.page_part)

            if args.page_type == 'operational':
                from transform.tokopedia.seller_center.operational import transform_operational_page
                transform_operational_page.main(args.os_key)

            if args.page_type == 'shop_score':
                from transform.tokopedia.seller_center.shop_score import transform_shop_score_page
                transform_shop_score_page.main(args.os_key)

            if args.page_type == "ads":
                from transform.tokopedia.seller_center.ads import transform_ads_page
                transform_ads_page.main(args.os_key)
                
    if args.marketplace == "tiktok":
        if args.job_type == "extract":
            if args.page_type == "seller_center":
                from extract.tiktok.seller_center import main
                main.execute(args.store_name, args.start_date, args.end_date)

    if args.marketplace == "propsid":
        if args.job_type == "extract":
            if args.page_type == "publisher":
                from extract.propsid import main
                main.execute(args.username, args.password)
