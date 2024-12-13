from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
}

BUCKET_NAME = "grupo3-202410"
CSV_FILE_PATH = "/tmp/stock.csv"
S3_OBJECT_NAME = "landing/stock/stock"
LAST_EXTRACTION_VAR = "last_extraction_date_stock"  # Variable de Airflow para la última fecha de extracción

# Define the DAG
@dag(dag_id="dag_stock", default_args=default_args, schedule_interval='@daily', start_date=days_ago(1), catchup=False, tags=['mysql_airflow_stock'])
def mysql_example_dag():

    # Task 1: Extract data from MySQL
    @task
    def extract_data_from_mysql():
        # Create a MySQL hook to connect to the database
        mysql_hook = MySqlHook(mysql_conn_id='mysql_conn_id')
        
        # Obtener la última fecha de extracción desde las Variables de Airflow
        last_extraction_date = Variable.get(LAST_EXTRACTION_VAR, default_var=None)

        # Crear la consulta SQL dinámica
        if last_extraction_date:
            query = f"""
                SELECT * FROM `bd-grupo-3-v2`.Stock
                WHERE fecha_extraccion > '{last_extraction_date}'
                ORDER BY fecha_extraccion DESC
            """
        else:
            query = "SELECT * FROM `bd-grupo-3-v2`.Stock ORDER BY fecha_extraccion DESC"
        
        # Run the query and fetch results
        connection = mysql_hook.get_conn()
        cursor = connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        connection.close()

        # Return the fetched rows
        print(f"Extracted {len(rows)} rows from MySQL.")
        return rows

    # Task 2: Transform the extracted data
    @task
    def transform_data(data):
        transformed_data = []
        for row in data:
            transformed_data.append({
                "id_stock": row[0],
                "id_producto": row[1],
                "stock_actual": row[2],
                "stock_valorizado": row[3],
                "fecha_extraccion": row[4]  # Campo de fecha de extracción
            })
        print(f"Transformed data: {transformed_data}")
        return transformed_data

    # Task 3: Create CSV file
    @task
    def create_csv(transformed_data):
        try:
            if not transformed_data:
                raise ValueError("No data available to create CSV.")
            
            # Obtener la última fecha de los registros transformados
            last_extraction_date = max([row['fecha_extraccion'] for row in transformed_data])
            s3_object_name = f"{S3_OBJECT_NAME}_{last_extraction_date.strftime('%Y-%m-%d_%H-%M-%S')}.csv"

            # Crear el archivo CSV
            with open(CSV_FILE_PATH, 'w') as file:
                file.write("id_stock,id_producto,stock_actual,stock_valorizado,fecha_extraccion\n")
                for row in transformed_data:
                    file.write(f"{row['id_stock']},{row['id_producto']},{row['stock_actual']},{row['stock_valorizado']},{row['fecha_extraccion']}\n")
            print(f"Created CSV file: {CSV_FILE_PATH}")
            return {"csv_file_path": CSV_FILE_PATH, "s3_object_name": s3_object_name, "last_extraction_date": last_extraction_date}
        except Exception as e:
            print(f"Error creating CSV file: {e}")
            raise

    # Task 4: Upload CSV to S3
    @task
    def upload_csv_to_s3(csv_object_data):
        s3_hook = S3Hook(aws_conn_id="aws_default")
        try:
            s3_hook.load_file(
                filename=csv_object_data.get("csv_file_path"),
                key=csv_object_data.get("s3_object_name"),
                bucket_name=BUCKET_NAME,
                replace=True
            )
            print(f"Uploaded CSV to S3: {csv_object_data.get('s3_object_name')}")
        except Exception as e:
            print(f"Error uploading CSV to S3: {e}")
            raise

    # Task 5: Update last extraction date in Airflow Variable
    @task
    def update_last_extraction_date(csv_object_data):
        last_extraction_date = csv_object_data.get("last_extraction_date")

        # Actualiza la Variable de Airflow con la nueva fecha de extracción
        Variable.set(LAST_EXTRACTION_VAR, last_extraction_date.strftime('%Y-%m-%d %H:%M:%S'))
        print(f"Updated last extraction date to: {last_extraction_date}")

    # Define task dependencies
    data = extract_data_from_mysql()
    transformed_data = transform_data(data)
    csv_object_data = create_csv(transformed_data)
    upload_csv_to_s3(csv_object_data)
    update_last_extraction_date(csv_object_data)

# Instantiate the DAG
mysql_example_dag_dag = mysql_example_dag()
