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
CSV_FILE_PATH="/tmp/proveedor.csv"
S3_OBJECT_NAME="landing/customers/proveedor.csv"
transformed_data_global = []
# Define the DAG
@dag(dag_id="dag_proveedor",default_args=default_args, schedule_interval='@daily', start_date=days_ago(1), catchup=False, tags=['mysql_airflow_proveedor'])
def mysql_example_dag():

    # Task 1: Extract data from MySQL
    @task
    def extract_data_from_mysql():
        # Create a MySQL hook to connect to the database
        mysql_hook = MySqlHook(mysql_conn_id='mysql_conn_id')
        # Define the query to extract data
        query = "select * from `bd-grupo-3`.Proveedor"
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
               "id_proveedor": row[0],
                "tipo_documento": row[1],
                "nro_documento": row[2],
                "razon_social": row[3],
                "nombre": row[4],
                "apellido_pa": row[5],
                "apellido_ma": row[6],
                "banco": row[7],
                "cuenta": row[8],
                "cci": row[9],
                "direccion": row[10].replace('\n', '').replace('\r', '')  
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
                file.write("id_proveedor,tipo_documento,nro_documento,razon_social,nombre,apellido_pa,apellido_ma,banco,cuenta,cci,direccion\n")
                for row in rows:
                    file.write(f"{row['id_proveedor']},{row['tipo_documento']},{row['nro_documento']},{row['razon_social']},{row['nombre']},{row['apellido_pa']},{row['apellido_ma']},{row['banco']},{row['cuenta']},{row['cci']},{row['direccion']}\n")
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
