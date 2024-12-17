import sys
import logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim, when, current_timestamp

import boto3

# Initialize logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize SparkSession with Hudi support
spark = (SparkSession.builder
         .config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer')  # Kryo serializer
         .config('spark.sql.extensions', 'org.apache.spark.sql.hudi.HoodieSparkSessionExtension')
         .config("spark.sql.legacy.pathOptionBehavior.enabled", "true")
         .getOrCreate())

sc = spark.sparkContext
glueContext = GlueContext(sc)

def main():
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    # Log start
    logger.info(f"Starting Silver Layer job: {args['JOB_NAME']}")

    # Paths for Bronze and Silver
    bronze_path = "s3://grupo3-202410/bronze/customers/"  # Bronze layer input
    silver_path = "s3://grupo3-202410/silver/customers/"  # Silver layer output

    # 1. Read Bronze layer data (Hudi format)
    logger.info(f"Reading data from Bronze layer: {bronze_path}")
    bronze_df = spark.read.format("hudi").load(bronze_path)

    # 2. Clean and transform data
    logger.info("Cleaning and transforming data for Silver layer")

    silver_df = bronze_df \
        .dropDuplicates(["id_cliente"]) \
        .filter(col("correo").isNotNull()) \
        .withColumn("correo", lower(trim(col("correo")))) \
        .withColumn("nombre", trim(col("nombre"))) \
        .withColumn("apellido_pa", trim(col("apellido_pa"))) \
        .withColumn("apellido_ma", trim(col("apellido_ma"))) \
        .withColumn("direccion", when(col("direccion").isNull(), "No especificado").otherwise(trim(col("direccion"))))  # Dirección default

    # 3. Enriquecimiento: Añadir columna de fecha de procesamiento
    silver_df = silver_df.withColumn("fecha_procesamiento", current_timestamp())

    logger.info("Data transformation completed")

    # 4. Write data to Silver layer in Parquet format
    logger.info(f"Writing data to Silver layer at: {silver_path}")
    silver_df.write.mode("overwrite").parquet(silver_path)

    logger.info(f"Silver Layer job {args['JOB_NAME']} completed successfully")
    job.commit()

if __name__ == "__main__":
    main()
