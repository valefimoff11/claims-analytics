from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, to_date, col

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DecimalType
from pyspark.sql.types import ArrayType, DoubleType, BooleanType

claims_path = "E:\\pyprojects\\claims\\pythonProject\\testdata\\claims.csv"
contracts_path = "E:\\pyprojects\\claims\\pythonProject\\testdata\\contracts.csv"

# Using custom schema
claims_schema = StructType() \
      .add("SOURCE_SYSTEM",StringType(),False) \
      .add("CLAIM_ID",StringType(),False) \
      .add("CONTRACT_SOURCE_SYSTEM",StringType(),False) \
      .add("CONTRACT_ID",IntegerType(),False) \
      .add("CLAIM_TYPE",StringType(),True) \
      .add("DATE_OF_LOSS",StringType(),True) \
      .add("AMOUNT", DecimalType(16, 5), True) \
      .add("CREATION_DATE", StringType(), True)

contracts_schema = StructType() \
      .add("SOURCE_SYSTEM",StringType(),False) \
      .add("CONTRACT_ID",IntegerType(),False) \
      .add("CONTRACT_TYPE",StringType(),False) \
      .add("INSURED_PERIOD_FROM",StringType(),False) \
      .add("INSURED_PERIOD_TO",StringType(),True) \
      .add("CREATION_DATE",StringType(),True) \


spark = SparkSession.builder.appName("claims-transactions").getOrCreate()

claims_df = spark.read.format("csv") \
      .option("header", True) \
      .schema(claims_schema) \
      .load(claims_path)

claims_df = claims_df.withColumn("CREATION_DATE", to_timestamp("CREATION_DATE", "dd.MM.yyyy HH:mm"))
claims_df = claims_df.withColumn("DATE_OF_LOSS", to_date("DATE_OF_LOSS", "dd.MM.yyyy"))

claims_df.printSchema()
claims_df.show()

contracts_df = spark.read.format("csv") \
      .option("header", True) \
      .schema(contracts_schema) \
      .load(contracts_path)

contracts_df = contracts_df.withColumn("CREATION_DATE", to_timestamp("CREATION_DATE", "dd.MM.yyyy HH:mm"))
contracts_df = contracts_df.withColumn("INSURED_PERIOD_FROM", to_date("INSURED_PERIOD_FROM", "dd.MM.yyyy"))
contracts_df = contracts_df.withColumn("INSURED_PERIOD_TO", to_date("INSURED_PERIOD_TO", "dd.MM.yyyy"))

contracts_df.printSchema()
contracts_df.show()

transactions_df = contracts_df.join(claims_df, (contracts_df["SOURCE_SYSTEM"] == claims_df["CONTRACT_SOURCE_SYSTEM"]) &
   ( contracts_df["CONTRACT_ID"] == claims_df["CONTRACT_ID"])) \
    .withColumn("CLAIMS_CREATION_DATE",claims_df["CREATION_DATE"]) \
    .drop(claims_df["CREATION_DATE"]) \
    .drop(claims_df["CONTRACT_ID"])

transactions_df.show()