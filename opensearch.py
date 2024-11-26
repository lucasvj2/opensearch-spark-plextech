from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
from pyspark.sql.types import StructType, StructField, StringType, StructType, BooleanType
from opensearchpy import OpenSearch
import random
import os

# -------------------------------
# Configuration and Initialization
# -------------------------------

# Set a random seed for deterministic behavior
random.seed(42)

# AWS OpenSearch Domain and Credentials
aws_opensearch_host = "search-query-accerlerator-v3-e73f6xwa724cczxzlpgxmt4noq.aos.us-east-2.on.aws"
aws_opensearch_port = 443  # HTTPS port
aws_opensearch_user = "plextech"
aws_opensearch_pass = ""  # Consider using environment variables or secure storage for credentials
t = "org.opensearch.client:opensearch-spark-30_2.12:1.2.0"


print("Spark session created")

# Initialize OpenSearch client using opensearch-py (optional, for additional operations)
opensearch_client = OpenSearch(
    hosts=[{'host': aws_opensearch_host, 'port': aws_opensearch_port}],
    http_compress=True,  # Enables compression
    use_ssl=True,
    http_auth=(aws_opensearch_user, aws_opensearch_pass),
    verify_certs=True,  # It's recommended to verify SSL certificates in production
    ssl_assert_hostname=True,
    ssl_show_warn=True
)

home_directory = os.path.expanduser("~")

spark = SparkSession.builder \
    .appName("MySparkExtensionApp") \
    .config("spark.sql.extensions", "org.opensearch.flint.spark.FlintSparkExtensions") \
    .config("spark.sql.catalog.dev", "org.apache.spark.opensearch.catalog.OpenSearchCatalog") \
    .config("spark.jars", f"{home_directory}/.m2/repository/org/opensearch/opensearch-spark-standalone_2.12/0.6.0-SNAPSHOT/opensearch-spark-standalone_2.12-0.6.0-SNAPSHOT.jar") \
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
    

print("OpenSearch client created")

# -------------------------------
# Define Schema for JSON Data
# -------------------------------

# # Define the schema focusing only on the required fields
schema = StructType([
    StructField("@timestamp", StringType(), True),
    StructField("body", StringType(), True),
    StructField("event", StructType([
        StructField("result", StringType(), True),
        StructField("name", StringType(), True),
        StructField("domain", StringType(), True)
    ]), True)
])

df = spark.read.format("org.opensearch.spark.sql").option("opensearch.nodes", aws_opensearch_host).load("new_logs")
grouped_df = (
    df
    .groupby("body")
    .agg(count("*").alias("count"))
)

# Show the results
grouped_df.show()
spark.stop()