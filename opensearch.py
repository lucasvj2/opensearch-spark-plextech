from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, rand, lit
from pyspark.sql.types import StructType, StructField, StringType, BooleanType
from opensearchpy import OpenSearch
import random

# -------------------------------
# Configuration and Initialization
# -------------------------------

# Set a random seed for deterministic behavior
random.seed(42)

# AWS OpenSearch Domain and Credentials
aws_opensearch_host = "search-query-accerlerator-v3-e73f6xwa724cczxzlpgxmt4noq.aos.us-east-2.on.aws"
aws_opensearch_port = 443
aws_opensearch_user = "plextech"
aws_opensearch_pass = "MasterPassword1!"

# Initialize Spark session
spark = SparkSession.builder \
    .appName("ExpensiveOperationsTest") \
    .config("spark.sql.extensions", "org.opensearch.flint.spark.FlintSparkExtensions") \
    .config("spark.sql.catalog.dev", "org.apache.spark.opensearch.catalog.OpenSearchCatalog") \
    .config("spark.jars", "/Users/dylanhopkins/.m2/repository/org/opensearch/opensearch-spark-standalone_2.12/0.6.0-SNAPSHOT/opensearch-spark-standalone_2.12-0.6.0-SNAPSHOT.jar") \
    .config("spark.jars.packages", "org.opensearch.client:opensearch-spark-30_2.12:1.2.0") \
    .config("opensearch.nodes", aws_opensearch_host) \
    .config("opensearch.port", "443") \
    .config("opensearch.nodes.wan.only", "true") \
    .config("opensearch.net.ssl", "true") \
    .config("opensearch.net.http.auth.user", aws_opensearch_user) \
    .config("opensearch.net.http.auth.pass", aws_opensearch_pass) \
    .config("opensearch.rest.client.connection.timeout", "60000") \
    .config("opensearch.rest.client.socket.timeout", "60000") \
    .getOrCreate()

print("Spark session created")

# Initialize OpenSearch client (optional)
opensearch_client = OpenSearch(
    hosts=[{'host': aws_opensearch_host, 'port': aws_opensearch_port}],
    http_compress=True,
    use_ssl=True,
    http_auth=(aws_opensearch_user, aws_opensearch_pass),
    verify_certs=True,
    ssl_assert_hostname=True,
    ssl_show_warn=True
)

# -------------------------------
# Create Larger Dataset
# -------------------------------

# Define schema and generate random data
schema = StructType([
    StructField("id", StringType(), True),
    StructField("value", StringType(), True),
    StructField("category", StringType(), True),
])

data = [
    (f"id_{i}", f"value_{random.randint(1, 100)}", f"category_{random.randint(1, 10)}")
    for i in range(100000)  # Increase this number for more expensive operations
]

df = spark.createDataFrame(data, schema)

# -------------------------------
# Add Expensive Transformations
# -------------------------------

# Transformation 1: Filter rows
filtered_df = df.filter(col("value").contains("5"))

# Transformation 2: Add random columns
transformed_df = filtered_df.withColumn("random_col", rand())

# Transformation 3: Group by and aggregate
aggregated_df = transformed_df.groupBy("category").agg(
    expr("count(*) as count"),
    expr("avg(random_col) as avg_random"),
    expr("max(value) as max_value")
)

# Transformation 4: Join with itself
joined_df = aggregated_df.alias("df1").join(
    aggregated_df.alias("df2"),
    col("df1.category") == col("df2.category"),
    "inner"
)

# -------------------------------
# Force Expensive Computation
# -------------------------------

# Count the final number of rows
row_count = joined_df.count()
print(f"Final row count: {row_count}")

# Collect results to force execution
results = joined_df.collect()
print("Execution completed and results collected.")

# -------------------------------
# Write Data to AWS OpenSearch
# -------------------------------

# OpenSearch index name
opensearch_index = "expensive_operations_test"

# Write the final DataFrame to OpenSearch
joined_df.write \
    .format("org.opensearch.spark.sql") \
    .option("opensearch.nodes", aws_opensearch_host) \
    .option("opensearch.port", str(aws_opensearch_port)) \
    .option("opensearch.net.ssl", "true") \
    .option("opensearch.net.http.auth.user", aws_opensearch_user) \
    .option("opensearch.net.http.auth.pass", aws_opensearch_pass) \
    .option("opensearch.nodes.wan.only", "true") \
    .option("opensearch.index.auto.create", "true") \
    .option("opensearch.resource", opensearch_index) \
    .mode("overwrite") \
    .save()

print(f"Data successfully written to OpenSearch index '{opensearch_index}'")

# -------------------------------
# (Optional) Verify Data in OpenSearch
# -------------------------------

# Count documents in OpenSearch
count_response = opensearch_client.count(index=opensearch_index)
print(f"Number of documents in '{opensearch_index}': {count_response['count']}")

# Stop the Spark session
spark.stop()
