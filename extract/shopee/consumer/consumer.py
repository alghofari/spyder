from kafka import KafkaConsumer
from json import loads
from time import sleep

def main(topic, bootstrap_servers: str, auto_offset_reset: str ='latest' , enable_auto_commit: bool = True, group_id: str = 'my-group-id'):
    print("starting consumer with topic ",topic)
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=[bootstrap_servers],
        auto_offset_reset=auto_offset_reset,
        enable_auto_commit=enable_auto_commit,
        group_id=group_id,
        value_deserializer=lambda x: loads(x.decode('utf-8'))
    )
    print("checkpoint 1 consumer")
    while True:
        for event in consumer:
            print(event)
            event_data = event.value
            marketplace = event_data["marketplace"]
            job_type = event_data["job_type"]
            page_type = event_data["page_type"]
            start_page = event_data["start_page"]
            end_page = event_data["end_page"]
            bucket_name = event_data["bucket_name"]
            base_path = event_data["base_path"]
            run_date = event_data["run_date"]
            target_table = event_data["target_table"]
            schema_path = event_data["schema_path"] 
            retry_count = event_data["retry_count"]

            print(f"count retry number {retry_count}")
            if marketplace == "shopee":
                if topic == 'spyder_shopee_mall_item': 
                    shop_spec = event_data["shop_spec"]
                    from extract.shopee.shopee_mall.mall_item import main
                    print(f'Consumer received {topic} for shop {shop_spec}, Page {start_page} to {end_page} for Retry no.{retry_count}')
                    main(start_page, end_page, bucket_name, base_path, run_date, target_table, schema_path, shop_spec, 
                        bootstrap_servers, topic, retry_count)
                else: 
                    from extract.shopee.shopee_item.search_item import main
                    category_spec = event_data["category_spec"]
                    print(f'Consumer received {topic} for shop {category_spec}, Page {start_page} to {end_page} for Retry no.{retry_count}')
                    main(start_page, end_page, bucket_name, base_path, run_date, target_table, schema_path, category_spec, 
                        bootstrap_servers, topic, retry_count)
