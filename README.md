# Description

Data engineering project featuring a data pipeline orchestrated by Apache Airflow. It retrieves, cleans, and enriches accommodation data from two major Luxembourgish real estate websites: Athome.lu and Immotop.lu.

# How to run it on your machine ?

## Requirement

A Linux machine with at least 4 GB of RAM is required, although 8 GB is recommended.

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

5) Once connected, let's setup the following airflow variables by navigating to **Admin** > **Variables** :

![image](https://github.com/user-attachments/assets/a2fc4948-8ff6-451b-b342-95f8d11ba99c)

To get the opencage API key, go to https://opencagedata.com/, create an account or sign-in, then you will be able to generate an API key from the dashbaord.

7) Now, in the DAG list, you should see a DAG called **immo_dag**

# Data Pipeline

![image](https://github.com/user-attachments/assets/1619b7d2-63c1-4071-9850-f23b25517a8f)

_The Airflow DAG structure_

## 1) Extraction

__DAG tasks concerned :__ _extract_data_from_athome_lu_, _extract_data_from_immotop_lu_

I use BeautifulSoup to scrape content from Athome.lu and Immotop.lu. For Athome.lu, I pre-clean most of the data during retrieval, as I can manage it on a case-by-case basis. In contrast, for Immotop.lu, I cannot pre-clean the data because the features I need to extract are not known in advance.

To track data transformations throughout the pipeline, each task's output is saved as a CSV file in dedicated folders. For extraction-related tasks, the CSV files are stored in the _raw_ folder

## 2) Transformation

__DAG tasks concerned :__ _transform_data_from_athome_lu_, _transform_data_from_immotop_lu_

In this phase, the raw data extracted from the websites is cleaned and transformed into a structured format. This includes:

- Parsing numerical values
- Removing duplicate listings based on accommodation links
- Standardizing districts names and expositions
- Extracting additional features (e.g., garages, street names, and street numbers)
- Normalizing text formats (e.g : removing spaces and irrelevant substrings)
- Converting data types for consistency
- Dropping irrelevant rows and columns

The transformed data is saved as CSV files in the _cleaned_ folder.
