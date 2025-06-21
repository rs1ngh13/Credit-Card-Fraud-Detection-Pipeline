from pyspark.sql import SparkSession

#jdbc connection
jdbc_url = (
    "jdbc:sqlserver://fraud-detection-synapse-9613.sql.azuresynapse.net:1433;"
    "database=fraudpool;"
    "user=username;"
    "password=password-here;"
    "encrypt=true;"
    "trustServerCertificate=false;"
    "hostNameInCertificate=*.sql.azuresynapse.net;"
    "loginTimeout=30;"
)

#load data
spark.conf.set(
  "fs.azure.account.key.storageaccount9613.dfs.core.windows.net",
  "storage-account-key"
)
anomaly_df = spark.read.format("delta").load(
    "abfss://fraud-detection-container@storageaccount9613.dfs.core.windows.net/anomaly_data"
)

#write data to Synapse Table
(anomaly_df.write
    .format("com.databricks.spark.sqldw")
    .option("url", jdbc_url)
    .option("forwardSparkAzureStorageCredentials", "true")
    .option("dbtable", "FraudFlags")
    .option("tempDir", "abfss://fraud-detection-container@storageaccount9613.dfs.core.windows.net/synapse_temp")
    .mode("overwrite")
    .save()
)
