import sys
import logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
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

# Ensure Kryo serializer is set before GlueContext/SparkContext initialization
spark = (SparkSession.builder
         .config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer')  # Kryo serializer
         .config('spark.sql.hive.convertMetastoreParquet', 'false')
         .config('spark.sql.extensions', 'org.apache.spark.sql.hudi.HoodieSparkSessionExtension')
         .config('spark.sql.legacy.pathOptionBehavior.enabled', 'true')
         .config("fs.s3.maxRetries", "20")
         .getOrCreate())

sc = spark.sparkContext
glueContext = GlueContext(sc)

def main():
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    # Log the start of the job
    logger.info(f"Starting job: {args['JOB_NAME']}")

    # S3 URIs
    parquet_file_s3_uri = "s3://grupo3-202410/landing/customers/"  # Directory path for Parquet files
    output_path = "s3://grupo3-202410/bronze/customers/"  # Output path for Hudi files

    # List the Parquet files in the S3 directory
    s3_client = boto3.client('s3')
    bucket_name = 'grupo3-202410'
    prefix = 'landing/customers/'

    # List objects in the directory
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    if 'Contents' not in response:
        logger.error("No files found in the specified S3 directory.")
        return

    # Get all the Parquet files and sort them by file name (numeric order)
    parquet_files = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.parquet')]
    sorted_files = sorted(parquet_files, key=lambda x: int(x.split('_')[-1].split('.')[0]), reverse=True)

    # Select the latest file
    latest_parquet_file = sorted_files[0]  # The file with the highest number
    parquet_file_s3_uri = f"s3://{bucket_name}/{latest_parquet_file}"

    logger.info(f"Using latest Parquet file: {parquet_file_s3_uri}")

    # Log the reading of the Parquet file
    logger.info(f"Reading Parquet file from {parquet_file_s3_uri}")

    # Read the Parquet file from S3 into a DataFrame
    parquet_df = spark.read.parquet(parquet_file_s3_uri)

    logger.info(f"Total rows before filtering: {parquet_df.count()}")

    # Select and rename columns of interest
    parquet_df = parquet_df.select(
        col("id_cliente"),
        col("nombre"),
        col("apellido_pa"),
        col("apellido_ma"),
        col("direccion"),
        col("tipo_documento"),
        col("nro_documento"),
        col("correo")
    )

    # Convert columns to string for consistency
    parquet_df = parquet_df.withColumn("id_cliente", col("id_cliente").cast("string"))
    parquet_df = parquet_df.withColumn("nombre", col("nombre").cast("string"))
    parquet_df = parquet_df.withColumn("apellido_pa", col("apellido_pa").cast("string"))
    parquet_df = parquet_df.withColumn("apellido_ma", col("apellido_ma").cast("string"))
    parquet_df = parquet_df.withColumn("direccion", col("direccion").cast("string"))
    parquet_df = parquet_df.withColumn("tipo_documento", col("tipo_documento").cast("string"))
    parquet_df = parquet_df.withColumn("nro_documento", col("nro_documento").cast("string"))
    parquet_df = parquet_df.withColumn("correo", col("correo").cast("string"))

    # Filter out rows with null or empty critical columns
    logger.info("Filtering out rows with null or empty id_cliente")
    parquet_df_filtered = parquet_df.filter(
        col('id_cliente').isNotNull() & (col('id_cliente') != "")
    )
    logger.info(f"Total rows after filtering: {parquet_df_filtered.count()}")

    # Show the DataFrame content (optional, for debug purposes)
    parquet_df_filtered.show()

    hudi_options = {
        'hoodie.table.name': 'hudi_cliente_bronze',
        'hoodie.datasource.write.storage.type': 'COPY_ON_WRITE',  # Use COPY_ON_WRITE table type
        'hoodie.datasource.write.recordkey.field': 'id_cliente',  # Record key to uniquely identify rows
        'hoodie.datasource.write.partitionpath.field': 'id_cliente',  # Partition field
        'hoodie.datasource.write.table.name': 'cliente_bronze',
        'hoodie.datasource.write.operation': 'insert',  # Only insert new records
        'hoodie.datasource.write.hive_style_partitioning': 'true',  # Hive partitioning style
        'hoodie.insert.shuffle.parallelism': 2,  # Insert parallelism
        'path': output_path,  # Output path in S3 or other storage
        'hoodie.datasource.hive_sync.enable': 'true',  # Enable Hive syncing
        'hoodie.datasource.hive_sync.database': 'grupo03_database',  # Hive database name
        'hoodie.datasource.hive_sync.table': 'cliente_bronze',  # Hive table name
        'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.MultiPartKeysValueExtractor',  # Partition extractor
        'hoodie.datasource.hive_sync.use_jdbc': 'false',  # Use HMS instead of JDBC
        'hoodie.datasource.hive_sync.mode': 'hms',  # Use HMS for Hive syncing
        'hoodie.datasource.hive_sync.auto_create_database': 'true',  # Automatically create the database if not exists
    }

    # Log the writing to Hudi
    logger.info(f"Writing DataFrame to Hudi at {output_path}")

    # Write the DataFrame to Hudi
    parquet_df_filtered.write.format("hudi").options(**hudi_options).mode("overwrite").save(output_path)

    logger.info(f"Job {args['JOB_NAME']} completed successfully")

    job.commit()

if __name__ == "__main__":
    main()
