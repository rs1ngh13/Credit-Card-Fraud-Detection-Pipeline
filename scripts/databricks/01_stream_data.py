from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

#event hub connection string
connection_string = (
    "event-hub-endpoint"
    "event-hub-shared-access-key-name"
    "event-hub-shared-access-key"
    "event-hub-entity-path"
)

eventhubs_conf = {
    'eventhubs.connectionString': sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connection_string)
}

#transaction schema
schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("user_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("merchant_id", StringType()),
    StructField("location", StringType()),
    StructField("timestamp", StringType())
])

#read stream from event hub
raw_stream = (
    spark.readStream
    .format("eventhubs")
    .options(**eventhubs_conf)
    .load()
)

#flatten data
parsed_stream = (
    raw_stream
    .withColumn("body", col("body").cast("string"))
    .select(from_json(col("body"), schema).alias("data"))
    .select("data.*")
)

#mount container
spark.conf.set(
  "fs.azure.account.key.storageaccount9613.dfs.core.windows.net",
  "storage-account-key"
)

#write transactions to storage account
query = (
    parsed_stream.writeStream
    .format("delta")
    .option("checkpointLocation", "abfss://fraud-detection-container@storageaccount9613.dfs.core.windows.net/checkpoints/fraud")
    .start("abfss://fraud-detection-container@storageaccount9613.dfs.core.windows.net/fraud_data")
)
