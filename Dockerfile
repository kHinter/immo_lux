FROM astrocrpublic.azurecr.io/runtime:3.2-3

USER root
RUN apt-get update && apt-get install -y wget gnupg

# 2. On installe Chromium une fois que les libs Python sont prêtes
USER astro
RUN python -m playwright install chromium

# 3. On installe les dépendances Linux du navigateur
USER root
RUN python -m playwright install-deps chromium

# 4. On redonne les droits à Astro pour Airflow
USER astro