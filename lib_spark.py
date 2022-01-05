# PySpark dependencies
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
from pyspark.sql import SQLContext
from pyspark.sql.functions import isnan, when, count, col, udf, dayofmonth, dayofweek, month, year, weekofyear
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import *

# Pandas and data-plotting dependencies
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import plotly.plotly as py
import plotly.graph_objs as go
import requests
requests.packages.urllib3.disable_warnings()

# System dependencies and others
import os
import configparser
import datetime as dt


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
    This method is designed to clean the inmigration dataset passed as PySpark dataframe

    :param df: I94 immigration dataset
    :type pyspark.sql.DataFrame:

    :return pyspark.sql.DataFrame:
    """

    print(f'Number of records present in the raw inmigration dataset: {df.count()}')
    
    # Dropping rows where all elements are NaN
    df = df.dropna(how='all')

    # Dropping columns ['occup', 'entdepu','insnum'] where almost all the data is NaN
    nan_columns = ['occup', 'entdepu','insnum']
    df = df.drop(*nan_columns)
    
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
    
    df = df.dropna(subset=['AverageTemperature'])
    df = df.drop_duplicates(subset=['dt', 'City', 'Country'])

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


