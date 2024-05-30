from pyspark.sql.functions import col, count, lit, max as spark_max, expr, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import os


def read_json_files(spark, directory_path, schema):
    """Json to Spark data frame."""
    data_frames = []
    json_files = [os.path.join(directory_path, f) for f in os.listdir(directory_path) if f.endswith(".json")]

    for file in json_files:
        data_frame = spark.read.schema(schema).json(file)
        repo_name = os.path.basename(file).replace("_pull_requests.json", '')
        data_frame = data_frame.withColumn("repository_id", col("repository_id")) \
            .withColumn("repository_name", lit(repo_name)) \
            .withColumn("repository_owner", col("repository_owner")) \
            .withColumn("merged_at",
                        to_timestamp("merged_at", "yyyy-MM-dd'T'HH:mm:ssXXX"))
        data_frames.append(data_frame)

    return data_frames


def combine_frames(data_frames):
    """Combine frames by groupBy"""
    combined_df = data_frames[0]
    for data_frame in data_frames[1:]:
        combined_df = combined_df.union(data_frame)

    grouped_df = combined_df.groupBy("organization_name", "repository_id", "repository_name", "repository_owner")

    result_df = grouped_df.agg(
        count("id").alias("num_prs"),
        count("merged_at").alias("num_prs_merged"),
        spark_max("merged_at").alias("latest_merged_at"),
        expr("(num_prs = num_prs_merged) AND (lower(repository_owner) like '%scytale%')").alias("is_compliant")
    )

    return result_df


def save_data(result_df, output_dir):
    """Parquet to files"""
    result_df.write.mode("overwrite").parquet(output_dir)
    print(f"Data saved to {output_dir}")


def prs_data_to_parquet(spark, json_dir, parquet_dir):
    """Main body"""
    schema = StructType([
        StructField("organization_name", StringType(), False),
        StructField("repository_id", IntegerType(), False),
        StructField("repository_name", StringType(), False),
        StructField("repository_owner", StringType(), False),
        StructField("merged_at", StringType(), True),
        StructField("id", IntegerType(), False),
    ])

    dfs = read_json_files(spark, json_dir, schema)
    result_df = combine_frames(dfs)
    save_data(result_df, parquet_dir)


def read_parquet(spark, path):
    """Read_parquet"""
    df = spark.read.parquet(path)
    df.show()
