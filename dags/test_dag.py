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

BUCKET_NAME="aws-bucket-grupo3"
CSV_FILE_PATH="/tmp/operaciones.csv"
S3_OBJECT_NAME="landing/customers/clientes.csv"
transformed_data_global = []
# Define the DAG
@dag(default_args=default_args, schedule_interval='@daily', start_date=days_ago(1), catchup=False, tags=['mysql_example'])
def mysql_example_dag():

    # Task 1: Extract data from MySQL
    @task
    def extract_data_from_mysql():
        # Create a MySQL hook to connect to the database
        mysql_hook = MySqlHook(mysql_conn_id='mysql_conn_id')
        # Define the query to extract data
        query = "select * from db_grupo_03.Cliente"
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
    #@task
    #def transform_data(data):
    #    transformed_data = []
    #    for row in data:
    #        transformed_data.append({
    #            "id": row[0],
    #            "name": f"test1_{row[1].upper()}",  # Example transformation: uppercase the name
    #            "value": row[2]    # Example transformation: multiply value by 100
    #        })
    #    print(f"Transformed data: {transformed_data}")
    #    return transformed_data


    #Crear archivo csv
    @task
    def create_csv(transformed_data):
        try:
            rows = transformed_data
            print(f"Transformed data: {rows}")
            with open(CSV_FILE_PATH, 'w') as file:
                file.write(str(rows))
                #file.write("id,name,value\n")
                #for row in rows:
                #    file.write(f"{row['id']},{row['name']},{row['value']}\n")
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
    csv_file_path = create_csv(data)
    upload_csv_to_s3(csv_file_path)
    #transformed_data = transform_data(data)

# Instantiate the DAG
mysql_example_dag_dag = mysql_example_dag()