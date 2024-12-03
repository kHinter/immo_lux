# Description

Immo Lux is a data engineering project featuring a data pipeline orchestrated by Apache Airflow. It retrieves, cleans, and enriches accommodation data from two major Luxembourgish real estate websites: Athome.lu and Immotop.lu.

# Data Pipeline

![image](https://github.com/user-attachments/assets/b1aef2e1-0a1e-47f9-8ca7-47cb315b45bb)
_The Airflow DAG structure_

## 1) Extraction

I use BeautifulSoup to scrape content from Athome.lu and Immotop.lu. For Athome.lu, I pre-clean most of the data during retrieval, as I can manage it on a case-by-case basis. In contrast, for Immotop.lu, I cannot pre-clean the data because the features I need to extract are not known in advance.
