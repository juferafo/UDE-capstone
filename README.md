# UDE-capstone

## Project Summary

The work presented in this repository illustrate an extract-transform-load (ETL) process designed to build a data model where data analysts can retreive insights on inmigration and find correlations between the incoming migration to the USA, the global land temperature data and the USA demographic information. In industry, ETLs are widely used to wrangle and enrich raw data before ingesting it into a Data Warehouse (DWH).

This project is organized as follows:

* Step 1: Scope of the project and data gatherig
* Step 2: Exploratory data analysis (EDA)
* Step 3: Data model definition
* Step 4: Run the ETL to model the data
* Step 5: Project write-up

We use the Python API of [Apache Spark](https://spark.apache.org/) ([PySpark](https://spark.apache.org/docs/latest/api/python/index.html)) to process the raw data and carry out any transformation related to it due to the efficiency of this data processing framework. We will load the corresponding datasets as Spark dataframes and perform EDA on them to locate the missing/duplicate values and clean the raw data.

Once the raw data is clean, We will proceed to re-organize it by constructing a database with the shape of a [star-schema](https://www.guru99.com/star-snowflake-data-warehousing.html). A star-schema is a widely used structure to organize the data hosted in Data Warehouses. They are characterized by a *fact* table that in our case will encapsulate the immigration dataset and by a set of *dimension* tables connected to the previous one by foreign keys. The *dimension* tables provide additional details that enrich the information present in the fact table. In this use-case, the *dimension* tables will be the *immigration calendar*, the *country*, the *visa* and the *demographics* tables.

In short, we will go through the following steps. 

1. The data will be loaded as PySpark [dataframes](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.html).
2. EDA on the I94 immigration dataset
3. EDA on the demographics dataset
4. EDA on the global land temperatures dataset
5. Once we know the distribution of missing values (NaNs or NULLs) we will proceed to clean the datasets mentioned above
6. Create the fact and dimension tables
7. Save the data as parquet files

We also explore future paths to continue with this project with the following scenarios:

1. What if the data was increased by 100x?

Apache Spark is a very powerful tool when it comes to huge amounts of data since this framework was designed to carry out heavy calculations that are performed in memory and in parallel. However, in order to profit from the parallelization of tasks, one must consider to execute the pipeline Python code in a Cluster. A good option it would be to run the ETL in Databricks or in AWS EMR cluster. One would gain efficiency also by moving the Data Model and Raw data into S3 due to the fast connection between the EMR cluster and S3 within the Amazon network.

2. What if the pipelines would be run on a daily basis by 7 am every day?

In order to implement this, we should consider an orchestration tool such as Apache Airflow. Apache Airflow can be used to generate a pipeline called DAG (that stands for diagrammatic acyclic graph) and execute this one at a regular basis (like at 7 am every day). The steps of this DAG are connected but there cannot be any loop. One could use Apache Airflow to trigger Apache Spark jobs like the one presented in this notebook on an EMR cluster on a daily basis.

3. What if the database needed to be accessed by 100+ people?

In this case I would move the whole project to the cloud using S3 for staging, EMR for carrying out the ETL and Redshift as the end place for the Data Model. This allows accessibility and scalability based on the user's demands.

The ETL pipeline and the helper Python methods can be found in the notebook `./Capstone-project.ipynb` and in `lib_spark.py`
