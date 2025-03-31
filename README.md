# Description

Data engineering project featuring a data pipeline orchestrated by Apache Airflow. It retrieves, cleans, and enriches accommodation data from two major Luxembourgish real estate websites: Athome.lu and Immotop.lu.

# How to run it on your machine ?

## Requirement

A Linux machine with at least 4 GB of RAM is required, although 8 GB is recommended.

## Installation & Deployment

1) Installation and deployment is very simple : just run the **init.sh** bash script located at the root of the project. It will install airflowctl, a CLI tool for managing Airflow projects, initializes a new project, and builds and starts an Airflow standalone instance in the background.
  
2) Access the Airflow Web UI at http://localhost:8080/ if you're using a local machine. If installed on a server, replace `localhost` with your server's IP address or domain name.

3) Once you reach the sign-in interface, log in using the admin credentials. Both the username and password are set to `airflow`.

4) Once connected, let's setup the following airflow variables by navigating to **Admin** > **Variables** :

![image](https://github.com/user-attachments/assets/a2fc4948-8ff6-451b-b342-95f8d11ba99c)

To get the opencage API key, go to https://opencagedata.com/, create an account or sign-in, then you will be able to generate an API key from the dashbaord.

5) Now, in the DAG list, you should see a DAG called **immo_dag**

# Data Pipeline

![image](https://github.com/user-attachments/assets/2cafd2e0-846d-4c77-899b-cc276d8ee366)

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
- Handling outliers and invalid values (e.g., replacing apartment surface areas below 9m² with NA)

The transformed data is saved as CSV files in the _cleaned_ folder.

## 3) Enrichment

__DAG tasks concerned :__ _athome_lu_data_enrichment_, _immotop_lu_data_enrichment_

In this phase, additional features are extracted from the accomodation descriptions using regex in order to :
1) Fill gaps where data is missing from one website's raw data but available on another
2) Increase dataset completeness and make the most of the collected data

The extracted features include, but are not limited to:

- Garden, balcony and terrace surface area
- Has_lift, Is_flat, Has_cellar, Has_balcony, Has_garden, Has_terrace

## 4) Data quality

__DAG tasks concerned :__ _verify_no_data_loss_after_athome_lu_data_enrichment_, _verify_no_data_loss_after_immotop_lu_data_enrichment_, _verify_columns_are_in_gx_dq_suite_, _gx_dq_validation_

1) The tasks starting with "verify_no_data_loss_after" automatically ensure that no existing values were mistakenly replaced with NA during the data enrichment process.
2) The "verify_columns_are_in_gx_dq_suite" task ensures that all columns in the merged dataframe are included in the GreatExpectations data quality checks


