from pyspark.sql import SparkSession
from src.transform import prs_data_to_parquet, read_parquet
from src.extract import extract_and_save_pr_data
from config import *


def initialize_spark_session():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("GitHub PR Data Processing") \
        .getOrCreate()


def main():
    spark = initialize_spark_session()

    extract_and_save_pr_data(ACCESS_TOKEN, ORGANIZATION_NAME, PARQUET_DIRECTORY)
    prs_data_to_parquet(spark, JSON_DIRECTORY, PARQUET_DIRECTORY)

    read_parquet(spark, PARQUET_DIRECTORY)

    spark.stop()


if __name__ == "__main__":
    main()
