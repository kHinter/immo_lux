FROM mcr.microsoft.com/playwright/python:v1.61.0-noble

WORKDIR /app

COPY athome/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY athome/athome_scraping.py .

CMD ["python", "athome_scraping.py"]