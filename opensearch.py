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
aws_opensearch_pass = "MasterPassword1!"  # Consider using environment variables or secure storage for credentials
t = "org.opensearch.client:opensearch-spark-30_2.12:1.2.0"


# # Create a Spark session for AWS OpenSearch
# spark = SparkSession.builder \
#     .appName("OpenSearch-Spark Deterministic Test") \
#     .config("spark.jars.packages", t) \
#     .config("spark.es.nodes", aws_opensearch_host) \
#     .config("spark.es.port", "443") \
#     .config("spark.es.net.ssl", "true") \
#     .config("spark.es.net.http.auth.user", "plextech") \
#     .config("spark.es.net.http.auth.pass", "MasterPassword1!") \
#     .getOrCreate()

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
# # -------------------------------
# # Load and Parse JSON Data
# # -------------------------------

# # Example JSON data (you can replace this with reading from a file or other sources)
# json_data = [
#     """{
#         "@timestamp": "2023-07-17T08:14:05.000Z",
#         "body": "2 111111111111 eni-0e250409d410e1290 162.142.125.177 10.0.0.200 38471 12313 6 1 44 1674898496 1674898507 ACCEPT OK",
#         "event": {
#           "result": "ACCEPT",
#           "name": "flow_log",
#           "domain": "vpc.flow_log"
#         },
#         "attributes": {
#           "data_stream": {
#             "dataset": "vpc.flow_log",
#             "namespace": "production",
#             "type": "logs_vpc"
#           }
#         },
#         "cloud": {
#           "provider": "aws",
#           "account": {
#             "id": "111111111111"
#           },
#           "region": "ap-southeast-2",
#           "resource_id": "vpc-0d4d4e82b7d743527",
#           "platform": "aws_vpc"
#         },
#         "aws": {
#           "s3": {
#             "bucket": "centralizedlogging-loghubloggingbucket0fa53b76-t57zyhgb8c2",
#             "key": "AWSLogs/111111111111/vpcflowlogs/us-east-2/2023/01/28/111111111111_vpcflowlogs_us-east-2_fl-023c6afa025ee5a04_20230128T0930Z_3a9dfd9d.log.gz"
#           },
#           "vpc": {
#             "version" : "2",
#             "account-id" : "111111111111",
#             "interface-id" : "eni-0e250409d410e1290",
#             "region": "ap-southeast-2",
#             "vpc-id": "vpc-0d4d4e82b7d743527",
#             "subnet-id": "subnet-aaaaaaaa012345678",
#             "az-id": "apse2-az3",
#             "instance-id": "i-0c50d5961bcb2d47b",
#             "srcaddr" : "162.142.125.177",
#             "dstaddr" : "10.0.0.200",
#             "srcport" : 38471,
#             "dstport" : 12313,
#             "protocol" : "6",
#             "packets" : 1,
#             "bytes" : 44,
#             "pkt-src-aws-service": "S3",
#             "pkt-dst-aws-service": "-",
#             "flow-direction": "ingress",
#             "start" : "1674898496",
#             "end" : "1674898507",
#             "action" : "ACCEPT",
#             "log-status" : "OK"
#           }
#         },
#         "communication": {
#           "source": {
#             "address": "162.142.125.177",
#             "port": 38471,
#             "packets": 1,
#             "bytes" : 44
#           },
#           "destination": {
#             "address": "10.0.0.200",
#             "port": 12313
#           }
#         }
#       }""",
#     """{
#         "@timestamp": "2023-07-17T08:14:05.000Z",
#         "body": "2 111111111111 eni-0e250409d410e1290 162.142.125.177 10.0.0.200 38471 12313 6 1 44 1674898496 1674898507 ACCEPT OK",
#         "event": {
#           "result": "ACCEPT",
#           "name": "flow_log",
#           "domain": "vpc.flow_log"
#         },
#         "attributes": {
#           "data_stream": {
#             "dataset": "vpc.flow_log",
#             "namespace": "production",
#             "type": "logs_vpc"
#           }
#         },
#         "cloud": {
#           "provider": "aws",
#           "account": {
#             "id": "111111111111"
#           },
#           "region": "ap-southeast-2",
#           "resource_id": "vpc-0d4d4e82b7d743527",
#           "platform": "aws_vpc"
#         },
#         "aws": {
#           "s3": {
#             "bucket": "centralizedlogging-loghubloggingbucket0fa53b76-t57zyhgb8c2",
#             "key": "AWSLogs/111111111111/vpcflowlogs/us-east-2/2023/01/28/111111111111_vpcflowlogs_us-east-2_fl-023c6afa025ee5a04_20230128T0930Z_3a9dfd9d.log.gz"
#           },
#           "vpc": {
#             "version" : "2",
#             "account-id" : "111111111111",
#             "interface-id" : "eni-0e250409d410e1290",
#             "region": "ap-southeast-2",
#             "vpc-id": "vpc-0d4d4e82b7d743527",
#             "subnet-id": "subnet-aaaaaaaa012345678",
#             "az-id": "apse2-az3",
#             "instance-id": "i-0c50d5961bcb2d47b",
#             "srcaddr" : "10.0.0.200",
#             "dstaddr" : "162.142.125.177",
#             "srcport" : 12313,
#             "dstport" : 38471,
#             "protocol" : "6",
#             "packets" : 1,
#             "bytes" : 440,
#             "pkt-src-aws-service": "-",
#             "pkt-dst-aws-service": "S3",
#             "flow-direction": "egress",
#             "start" : "1674898496",
#             "end" : "1674898507",
#             "action" : "REJECT",
#             "log-status" : "OK"
#           }
#         },
#         "communication": {
#           "source": {
#             "address": "10.0.0.200",
#             "port": 12313,
#             "packets": 1,
#             "bytes" : 440
#           },
#           "destination": {
#             "address": "162.142.125.177",
#             "port": 38471
#           }
#         }
#       }"""
# ]

# # Create an RDD from the JSON strings
# rdd = spark.sparkContext.parallelize(json_data)

# # Read the JSON data into a DataFrame using the defined schema
# df = spark.read.schema(schema).json(rdd)

# # Select the required fields
# selected_df = df.select(
#     col("@timestamp"),
#     col("body"),
#     col("event")
# )

# # Show the DataFrame (optional, for verification)
# selected_df.show(truncate=False)

# # -------------------------------
# # Write Data to AWS OpenSearch
# # -------------------------------

# # OpenSearch index name
# opensearch_index = "flow_logs_index"  # Change this to your desired index name

# # Write the DataFrame to OpenSearch
# selected_df.write \
#     .format("org.opensearch.spark.sql") \
#     .option("opensearch.nodes", aws_opensearch_host) \
#     .option("opensearch.port", str(aws_opensearch_port)) \
#     .option("opensearch.net.ssl", "true") \
#     .option("opensearch.net.http.auth.user", aws_opensearch_user) \
#     .option("opensearch.net.http.auth.pass", aws_opensearch_pass) \
#     .option("opensearch.nodes.wan.only", "true") \
#     .option("opensearch.index.auto.create", "true") \
#     .option("opensearch.resource", opensearch_index) \
#     .option("opensearch.mapping.id", "event.domain") \
#     .mode("append") \
#     .save()

# print(f"Data successfully written to OpenSearch index '{opensearch_index}'")

# # -------------------------------
# # (Optional) Verify Data in OpenSearch
# # -------------------------------

# # Example: Retrieve the count of documents in the index
# count_response = opensearch_client.count(index=opensearch_index)
# print(f"Number of documents in '{opensearch_index}': {count_response['count']}")

# # Stop the Spark session
# spark.stop()