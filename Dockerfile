FROM astrocrpublic.azurecr.io/runtime:3.2-3

USER root
RUN apt-get update && apt-get install -y wget gnupg

# 4. On redonne les droits à Astro pour Airflow
USER astro