from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize SparkSession
spark = SparkSession.builder.appName("DynamicSchemaExample").getOrCreate()

# List of fields and their data types
fields = [
    ("name", StringType()),
    ("age", IntegerType()),
    ("city", StringType())
]

# Create an empty StructType
schema = StructType()

# Iterate over the fields and add StructField to the schema
for field_name, field_type in fields:
    schema.add(StructField(field_name, field_type, True))

# Example data
data = [("Alice", 30, "New York"), ("Bob", 25, "San Francisco")]

# Create DataFrame with the dynamically created schema
df = spark.createDataFrame(data, schema)

# Show the DataFrame
df.show()

# Print the schema
df.printSchema()
