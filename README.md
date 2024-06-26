# Spyder

Spyder is an easy-to-use, reliable, and performant tool for scrapping marketplace data.

## Key Features

- Collecting category, item, and shop data from Shopee and Tokopedia.
- Transforming html data to insightful information.
- Load data to BigQuery

## Running locally

Spyder requires the following dependencies:

- Python (version 3.9 or above)
- Docker

Run the following commands to build Dockerfile

```shell
# docker build spyder:<version>
docker build -t spyder:1.0.0 .
```

Use the following command to run

```shell
docker run \
  --name spyder \
  --rm \
  --volume /Users/user/PycharmProjects/spyder/config/:/app/config/ \
  --env GOOGLE_APPLICATION_CREDENTIALS=/app/config/spyder_sa.json \
  spyder:1.0.0 \
  --marketplace shopee \
  --job_type extract \
  --page_type category \
  --start_page 0 \
  --end_page 4 \
  --category_name Ayunan-Bayi-cat.11043350.11043411.11043413 \
  --bucket_name sirclo-data-marketplace \
  --base_path "assets/html/shopee/category"
  
docker run \
  --name spyder \
  --rm \
  --volume /Users/user/PycharmProjects/spyder/config/:/app/config/ \
  --env GOOGLE_APPLICATION_CREDENTIALS=/app/config/spyder_sa.json \
  spyder:1.0.0 \
  --marketplace shopee \
  --job_type transform \
  --page_type category \
  --category_name Ayunan-Bayi-cat.11043350.11043411.11043413 \
  --base_path "assets/html/shopee/category" \
  --bucket_name sirclo-data-marketplace \
  --target_table sirclo-prod.bronze_marketplace.shopee_item_header
```
