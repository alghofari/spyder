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

## GCP Services

- Project: sirclo-data-prod
- Cloud Storage: https://console.cloud.google.com/storage/browser/sirclo-data-marketplace
- BigQuery: sirclo-prod.bronze_marketplace
- Artifact Registry: asia-southeast1-docker.pkg.dev/sirclo-data-prod/data-service/spyder
- Kubernetes Engine: data-prod

## Artifact Registry

Run the following command to push local docker image to artifact registry
```shell
docker tag spyder:1.0.0 asia-southeast1-docker.pkg.dev/sirclo-data-prod/data-service/spyder:1.0.0
docker push asia-southeast1-docker.pkg.dev/sirclo-data-prod/data-service/spyder:1.0.0
```

## Deploy on Kubernetes

Requirements:

- kubectl: https://kubernetes.io/docs/tasks/tools/
- gcloud: https://cloud.google.com/sdk/docs/install

Run the following command to initialize the gcloud CLI:
```shell
# initialize gcloud cli
gcloud init

# show project list
gcloud projects list

# switch to intended project
gcloud config set project sirclo-data-prod
```

Use the following command to get artifactory registry and kube config credentials:
```shell
# get artifactory registry config
gcloud auth configure-docker asia-southeast1-docker.pkg.dev

# get kube config
gcloud container clusters get-credentials data-prod --zone=asia-southeast1-a
```

Add new cronjob manifest file:
```
for extract cronjob, store the file on this directory:
./releases/extract/shopee/

for transform cronjob, store the file on this directory:
./releases/transform/shopee/

format file name:
cronjob_<category_name>.yaml

notes: for now, we extract the html page maximum until page 5
```

Create and list cronjob on kubernetes:
```shell
# create cronjob
kubectl create -f ./releases/extract/shopee -n spyder
kubectl create -f ./releases/transform/shopee -n spyder

# show cronjob list
kubectl get cronjob -n spyder
```

Cronjob operations:
```shell
# if cronjob have meet the interval, to show the job list use this command
kubectl get jobs -n spyder

# show kubernetes pods
kubectl get pods -n spyder

# delete running job
kubectl delete jobs/<job-name> -n spyder

# delete cronjob
kubectl delete cronjob/<cronjob-name> -n spyder
```

Error tracing:
```shell
kubectl logs <pod-name> -n spyder
kubectl describe pods <pod-name> -n spyder
```

Access link below to run ci/cd pipeline spyder

```shell
https://jenkins.sirclo.net/job/Data/job/spyder/
```