from kafka import KafkaConsumer
from json import loads
from time import sleep
from helpers.time_helper import sleep_condition

def main(topic, bootstrap_servers: str = 'kafka-cp-kafka-headless.kafka:9092', auto_offset_reset: str ='latest' , enable_auto_commit: bool = True, group_id: str = 'my-group-id'):
    print("starting consumer with topic ",topic)
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=[bootstrap_servers],
        auto_offset_reset=auto_offset_reset,
        enable_auto_commit=enable_auto_commit,
        group_id=group_id,
        value_deserializer=lambda x: loads(x.decode('utf-8'))
    )
    
    while True:
        for event in consumer:
            event_data = event.value
            marketplace = event_data["marketplace"]
            job_type = event_data["job_type"]
            page_type = event_data["page_type"]
            start_page = event_data["start_page"]
            end_page = event_data["end_page"]
            category_name = event_data["category_name"]
            bucket_name = event_data["bucket_name"]
            base_path = event_data["base_path"]
            run_date = event_data["run_date"]
            sort_by = event_data["sort_by"]
            retry_count = event_data["retry_count"]
            if marketplace == "tokopedia":
                from extract.tokopedia.search_item.tokopedia_item import main
                print(f'Consumer received {topic} for category {category_name}, Page {start_page} to {end_page} for Retry no.{retry_count}')
                sleep_condition(60, 300)
                main(start_page,end_page,category_name,bucket_name,base_path,run_date,bootstrap_servers,topic,sort_by,retry_count)