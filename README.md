# Description

Data engineering project featuring a data pipeline orchestrated by Apache Airflow. It retrieves, cleans, and enriches accommodation data from two major Luxembourgish real estate websites: Athome.lu and Immotop.lu.

# How to run it on your machine ?

## Requirement

An Ubuntu or Debian Linux machine with at least 8 GB of RAM is required.

## Installation & Deployment

1) Installation is very simple : just run the **init.sh** Bash script located at the root of the project. 
There's no need to have Airflow or Python pre-installed, as the script is designed for quick deployment on servers.
By default airflow will be installed in `/home/airflow`.

2) Once fully executed, you can now launch the webserver and the scheduler by typing in the terminal the following commands :

```bash
airflow webserver &
airflow scheduler &
```

3) Access the Airflow Web UI at http://localhost:8080/ if you're using a local machine. If installed on a server, replace `localhost` with your server's IP address or domain name.

4) Once you reach the sign-in interface, log in using the admin credentials. Both the username and password are set to `airflow`.

5) Now, in the DAG list, you should see a DAG called **immo_dag**

6) Before running the DAG, navigate to the repository folder and type `docker compose up -d` in the terminal to start the Selenium Grid server

# Data Pipeline

![image](https://github.com/user-attachments/assets/d903cc4e-dcae-429c-8148-5146a1e5c2c0)

_The Airflow DAG structure_

## 1) Extraction

__DAG tasks and grouptasks concerned :__ _extract_data_from_athome_lu_, _extract_data_from_immotop_lu_

I use BeautifulSoup to scrape content from Athome.lu and Immotop.lu. For Athome.lu, I pre-clean most of the data during retrieval, as I can manage it on a case-by-case basis. In contrast, for Immotop.lu, I cannot pre-clean the data because the features I need to extract are not known in advance.

Since Athome.lu loads accommodation images dynamically, I use Selenium to retrieve image URLs. This makes the process significantly slower compared to Immotop.lu, which relies solely on BeautifulSoup.
To speed up scraping, I use a Selenium Grid server with two nodes, enabling concurrent execution of two DAG tasks (_scraping_part1_ and _scraping_part2_), each processing half of the website’s pages.

To track data transformations throughout the pipeline, each task's output is saved as a CSV file in dedicated folders. For extraction-related tasks, the CSV files are stored in the _raw_ folder
