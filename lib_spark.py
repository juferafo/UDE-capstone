# PySpark dependencies
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
from pyspark.sql import SQLContext
from pyspark.sql.functions import count, col, udf, monotonically_increasing_id
from pyspark.sql.functions import dayofmonth, dayofweek, month, year, weekofyear
from pyspark.sql.types import *

# Pandas and data-plotting dependencies
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

import os

def create_spark_session():
    """
    This method creates and returns the SparkSession
    """
        
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11") \
        .enableHiveSupport() \
        .getOrCreate()
    
    return spark

def print_nan_values(df):
    """
    This method is designed to print the number of missing values per column found in a 
    PySpark dataframe

    :param df: PySpark dataframe
    :type pyspark.sql.DataFrame:

    :return pandas.DataFrame:
    """

    df_nan = df.toPandas().isnull().sum()

    df_nan = pd.DataFrame(data = df_nan, columns=['null_values'])
    df_nan = df_nan.reset_index()
    df_nan.columns = ['columns', 'null_values']

    df_nan[df_nan['null_values'] > 0]

    return df_nan



def plot_nan_values(df) -> None:
    """
    This method is designed to show the missing values present in a PySpark dataframe
    
    :param df: PySpark dataframe
    :type pyspark.sql.DataFrame:
    """

    nans_df = df.\
        select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df.columns]).\
        toPandas()

    nans_df = pd.melt(nans_df, var_name='cols', value_name='values')
    total_nans = df.count()
    
    nans_df['percentage_of_missing_values'] = 100*nans_df['values']/total_nans
    
    plt.rcdefaults()
    plt.figure(figsize=(10,5))

    ax = sns.barplot(x="cols", y="percentage_of_missing_values", data=nans_df, color = 'blue')
    ax.set_ylim(0, 100)
    ax.set_xticklabels(ax.get_xticklabels(), rotation=90)

    plt.show()


def clean_immigration_dataset(df):
    """
    This method is designed to clean the immigration dataset passed as PySpark dataframe

    :param df: I94 immigration dataset
    :type pyspark.sql.DataFrame:

    :return pyspark.sql.DataFrame:
    """

    print(f'Number of records present in the raw immigration dataset: {df.count()}')
    
    # Dropping rows where all elements are NaN
    df = df.dropna(how='all')

    # Dropping columns ['occup', 'entdepu','insnum'] where almost all the data is NaN
    nan_columns = ['occup', 'entdepu','insnum']
    df = df.drop(*nan_columns)
    
    df = df.dropDuplicates(['cicid'])
    
    print(f'Number of records after cleaning: {df.count()}')
    
    return df


def clean_temperature_dataset(df):
    """
    This method is designed to clean the temperature dataset passed as PySpark dataframe
    
    :param df: Global temperature dataset
    :type pyspark.sql.DataFrame:

    :return pyspark.sql.DataFrame:
    """

    print(f'Number of records present in the raw temperature dataset: {df.count()}')
    
    nan_columns = ['AverageTemperature', 'AverageTemperatureUncertainty']    
    df = df.dropna(subset=nan_columns)
    
    duplicate_columns = ['dt', 'City', 'Country']
    df = df.drop_duplicates(subset=duplicate_columns)

    print(f'Number of records after cleaning: {df.count()}')
    
    return df


def clean_demographics_data(df):
    """
    This method is designed to clean the Demographics dataset passed as PySpark dataframe

    :param df: Global temperature dataset
    :type pyspark.sql.DataFrame:

    :return pyspark.sql.DataFrame:
    """

    print(f'Number of records present in the raw temperature dataset: {df.count()}')

    nan_columns = [
        'Male Population',
        'Female Population',
        'Number of Veterans',
        'Foreign-born',
        'Average Household Size'
    ]

    duplicate_columns = [
        'City', 
        'State', 
        'State Code', 
        'Race'
    ]

    df = df.dropna(subset=nan_columns)
    df = df.dropDuplicates(subset=duplicate_columns)

    print(f'Number of records after cleaning: {df.count()}')
    
    return df


def aggregate_temperature_data(df):
    """
    This method aggregates the temperature data by country
    
    :param df: Global temperature dataset
    :type pyspark.sql.DataFrame:

    :return pyspark.sql.DataFrame:
    """

    agg_df = df.\
        select(['Country', 'AverageTemperature']).\
        groupby('Country').avg()
    
    return agg_df.withColumnRenamed('avg(AverageTemperature)', 'average_temperature')


