from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
}

BUCKET_NAME = "grupo3-202410"
PARQUET_FILE_PATH = "/tmp/operaciones.parquet"
S3_BASE_PATH = "landing/shopping/compras"
LAST_EXTRACTION_VAR = "last_extraction_date_compra"  # Nombre de la Variable en Airflow
LAST_EXTRACTION_PART_VAR = "last_extraction_part_compra"  # Nueva variable para la parte de la extracción

# Define the DAG
@dag(
    dag_id="dag_compra",
    default_args=default_args,
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['mysql_airflow_compra']
)
def mysql_example_dag():

    # Task 1: Extract data from MySQL
    @task
    def extract_data_from_mysql():
        mysql_hook = MySqlHook(mysql_conn_id='mysql_conn_id')
        
        # Obtener la última fecha de extracción desde las Variables de Airflow
        last_extraction_date = Variable.get(LAST_EXTRACTION_VAR, default_var=None)

        # Crear la consulta SQL dinámica
        if last_extraction_date:
            query = f"""
                SELECT * FROM `bd-grupo-3-v3`.Compra 
                WHERE fecha_extraccion > '{last_extraction_date}' 
                ORDER BY fecha_extraccion DESC
            """
        else:
            query = "SELECT * FROM `bd-grupo-3-v3`.Compra ORDER BY fecha_extraccion DESC"

        # Ejecutar la consulta
        connection = mysql_hook.get_conn()
        cursor = connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        connection.close()

        print(f"Fetched {len(rows)} records from MySQL")
        return rows

    # Task 2: Transform the extracted data
    @task
    def transform_data(data):
        transformed_data = []
        for row in data:
            transformed_data.append({
                "id_compra": row[0],
                "id_proveedor": row[1],
                "cod_producto": row[2],
                "cantidad_compra": row[3],
                "costo_promedio_unitario": row[4],
                "monto_compra": row[5],
                "fecha_hora": row[6],
                "fecha_extraccion": row[7]
            })
        print(f"Transformed data: {transformed_data}")
        return transformed_data

    # Task 3: Crear archivo Parquet
    @task
    def create_parquet(transformed_data):
        try:
            if not transformed_data:
                raise ValueError("No data available to create Parquet.")

            # Convertir los datos transformados a una tabla de Apache Arrow
            table = pa.Table.from_pylist(transformed_data)

            # Obtener la última fecha de los registros transformados
            last_extraction_date = max([row['fecha_extraccion'] for row in transformed_data])

            # Obtener el valor actual de 'last_extraction_part' y actualizarlo
            last_extraction_part = Variable.get(LAST_EXTRACTION_PART_VAR, default_var=0)
            s3_object_name = f"{S3_BASE_PATH}_{last_extraction_part}.parquet"

            # Crear el archivo Parquet
            pq.write_table(table, PARQUET_FILE_PATH)
            print(f"Created Parquet file: {PARQUET_FILE_PATH}")
            return {"parquet_file_path": PARQUET_FILE_PATH, "s3_object_name": s3_object_name, "last_extraction_date": last_extraction_date, "last_extraction_part": last_extraction_part}
        except Exception as e:
            print(f"Error creating Parquet file: {e}")
            return None

    # Task 4: Subir Parquet a S3
    @task
    def upload_parquet_to_s3(parquet_object_data):
        s3_hook = S3Hook(aws_conn_id="aws_default")

        parquet_file_path = parquet_object_data.get("parquet_file_path")
        s3_object_name = parquet_object_data.get("s3_object_name")
        
        try:
            s3_hook.load_file(
                filename=parquet_file_path,
                key=s3_object_name,
                bucket_name=BUCKET_NAME,
                replace=True
            )
            print(f"Uploaded Parquet to S3: {s3_object_name}")
        except Exception as e:
            print(f"Error uploading Parquet to S3: {e}")

    # Task 5: Actualizar la Variable de Airflow
    @task
    def update_last_extraction_date(parquet_object_data):
        last_extraction_date = parquet_object_data.get("last_extraction_date")

        formatted_date = last_extraction_date.strftime('%Y-%m-%d_%H:%M:%S')

        # Actualiza la Variable de Airflow con la nueva fecha de extracción
        Variable.set(LAST_EXTRACTION_VAR, formatted_date)
        print(f"Updated last extraction date to: {formatted_date}")

    @task
    def update_last_extraction_part(parquet_object_data):
        # Incrementar last_extraction_part
        print('type parquet')
        print(type(parquet_object_data.get("last_extraction_part")))
        last_extraction_part = int(parquet_object_data.get("last_extraction_part"),0) + 1
        Variable.set(LAST_EXTRACTION_PART_VAR, str(last_extraction_part))
        print(f"Updated last extraction part to: {last_extraction_part}")

    # Define task dependencies
    data = extract_data_from_mysql()
    transformed_data = transform_data(data)
    parquet_object_data = create_parquet(transformed_data)
    upload_parquet_to_s3(parquet_object_data)
    update_last_extraction_date(parquet_object_data)
    update_last_extraction_part(parquet_object_data)

# Instantiate the DAG
mysql_example_dag_dag = mysql_example_dag()