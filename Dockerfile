FROM astrocrpublic.azurecr.io/runtime:3.2-3

USER root
RUN apt-get update && apt-get install -y wget gnupg kubectl
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg && apt-get update -y && apt-get install google-cloud-cli -y

# 4. On redonne les droits à Astro pour Airflow
USER astro