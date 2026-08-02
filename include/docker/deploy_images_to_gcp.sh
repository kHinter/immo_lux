#This script is used to deploy the docker images to the Google Cloud Artifact Registry

# Login to the Artifact Registry with Docker
cat ../sa_credentials/artifact_registry_credentials.json | docker login -u _json_key --password-stdin https://europe-west1-docker.pkg.dev

REGISTRY_NAME=europe-west1-docker.pkg.dev/lux-immo-438316/docker-images
APP_NAME=athome-scraper

#IMPORTANT : Always precise the platform because the most common platform is linux/amd64 and if you don't specify it, it will build for your local platform which is probably not linux/amd64 and the image will not work on GCP
docker buildx build \
    --file athome/Dockerfile \
    --tag $REGISTRY_NAME/$APP_NAME:v1 \
    --cache-from type=registry,ref=$REGISTRY_NAME/$APP_NAME:cache \
    --cache-to type=registry,ref=$REGISTRY_NAME/$APP_NAME:cache,mode=max \
    --platform linux/amd64 \
    --push .
