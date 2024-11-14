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

BUCKET_NAME="grupo3-202410"
CSV_FILE_PATH="/tmp/operaciones.csv"
S3_OBJECT_NAME="landing/customers/compra.csv"
transformed_data_global = []
# Define the DAG
@dag(dag_id="dag_compra",default_args=default_args, schedule_interval='@daily', start_date=days_ago(1), catchup=False, tags=['mysql_airflow_compra'])
def mysql_example_dag():

#Campos de compra
#id_compra,id_proveedor,cod_producto,cantidad_compra,costo_promedio_unitario,monto_compra,fecha_hora
    # Task 1: Extract data from MySQL
    @task
    def extract_data_from_mysql():
        # Create a MySQL hook to connect to the database
        mysql_hook = MySqlHook(mysql_conn_id='mysql_conn_id')
        # Define the query to extract data
        query = "select * from `bd-grupo-3`.Compra"
        # Run the query and fetch results
        connection = mysql_hook.get_conn()
        cursor = connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        connection.close()

        # Return the fetched rows
        print(f"Extracted rows: {rows}")
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
                "fecha_hora": row[6]
           })
       print(f"Transformed data: {transformed_data}")
       return transformed_data

    #Crear archivo csv
    @task
    def create_csv(transformed_data):
        try:
            rows = transformed_data
            print(f"Transformed data: {rows}")
            with open(CSV_FILE_PATH, 'w') as file:
                file.write("id_compra,id_proveedor,cod_producto,cantidad_compra,costo_promedio_unitario,monto_compra,fecha_hora\n")
                for row in rows:
                    file.write(f"{row['id_compra']},{row['id_proveedor']},{row['cod_producto']},{row['cantidad_compra']},{row['costo_promedio_unitario']},{row['monto_compra']},{row['fecha_hora']}\n")
            print(f"Created CSV file: {CSV_FILE_PATH}")
            return CSV_FILE_PATH
        except Exception as e:
            print(f"Error creating CSV file: {e}")

    @task
    def upload_csv_to_s3(csv_file_path):
        s3_hook = S3Hook(aws_conn_id="aws_default")
        try:
            s3_hook.load_file(
                filename=csv_file_path,
                key=S3_OBJECT_NAME,
                bucket_name=BUCKET_NAME,
                replace=True
            )
            print(f"Uploaded CSV to S3: {S3_OBJECT_NAME}")
        except Exception as e:
            print(f"Error uploading CSV to S3: {e}")

    #@task
    #def read_csv():
    #    with open('/tmp/operaciones.csv', 'r') as file:
    #        data = file.read()
    #    print(f"Read data from CSV: {data}")
    #    return data

    # Define task dependencies
    data = extract_data_from_mysql()
    transformed_data = transform_data(data)
    csv_file_path = create_csv(transformed_data)
    upload_csv_to_s3(csv_file_path)

# Instantiate the DAG
mysql_example_dag_dag = mysql_example_dag()
