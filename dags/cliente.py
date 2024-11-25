from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from datetime import datetime

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
}

BUCKET_NAME = "grupo3-202410"
CSV_FILE_PATH = "/tmp/operaciones.csv"
# Base path for the S3 object
S3_BASE_PATH = "landing/customers/"
LAST_EXTRACTION_VAR = "last_extraction_date_cliente"  # Nombre de la Variable en Airflow


@dag(dag_id="dag_cliente", default_args=default_args, schedule_interval='@daily', start_date=days_ago(1), catchup=False, tags=['mysql_airflow_cliente'])
def mysql_example_dag():

    @task
    def extract_data_from_mysql():
        mysql_hook = MySqlHook(mysql_conn_id='mysql_conn_id')

        # Obtener la última fecha de extracción desde las Variables de Airflow
        last_extraction_date = Variable.get(LAST_EXTRACTION_VAR, default_var=None)

        # Crear la consulta SQL dinámica
        if last_extraction_date:
            query = f"""
                SELECT * FROM `bd-grupo-3-v2`.Cliente 
                WHERE fecha_extraccion > '{last_extraction_date}' 
                ORDER BY fecha_extraccion DESC
            """
        else:
            query = "SELECT * FROM `bd-grupo-3-v2`.Cliente ORDER BY fecha_extraccion DESC"

        # Ejecutar la consulta
        connection = mysql_hook.get_conn()
        cursor = connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        connection.close()

        print(f"Fetched {len(rows)} records from MySQL")
        return rows

    @task
    def transform_data(data):
        transformed_data = []
        for row in data:
            transformed_data.append({
                "id_cliente": row[0],
                "nombre": row[1],
                "apellido_pa": row[2],
                "apellido_ma": row[3],
                "direccion": row[4].replace('\n', '').replace('\r', ''),
                "tipo_documento": row[5],
                "nro_documento": row[6],
                "correo": row[7],
                "fecha_extraccion": row[8],  # Asegúrate de incluir la columna en la transformación
            })
        print(f"Transformed data: {transformed_data}")
        return transformed_data

    @task
    def create_csv(transformed_data):
        try:
            if not transformed_data:
                raise ValueError("No data available to create CSV.")

            # Obtener la última fecha de los registros transformados
            last_extraction_date = max([row['fecha_extraccion'] for row in transformed_data])
            s3_object_name = f"{S3_BASE_PATH}clientes_{last_extraction_date.strftime('%Y-%m-%d_%H-%M-%S')}.csv"

            # Crear el archivo CSV
            with open(CSV_FILE_PATH, 'w') as file:
                file.write("id_cliente,nombre,apellido_pa,apellido_ma,direccion,tipo_documento,nro_documento,correo\n")
                for row in transformed_data:
                    file.write(f"{row['id_cliente']},{row['nombre']},{row['apellido_pa']},{row['apellido_ma']},{row['direccion']},{row['tipo_documento']},{row['nro_documento']},{row['correo']}\n")
            print(f"Created CSV file: {CSV_FILE_PATH}")
            return {"csv_file_path": CSV_FILE_PATH, "s3_object_name": s3_object_name, "last_extraction_date": last_extraction_date}
        except Exception as e:
            print(f"Error creating CSV file: {e}")
            return None

    @task
    def upload_csv_to_s3(csv_object_data):
        s3_hook = S3Hook(aws_conn_id="aws_default")

        csv_file_path= csv_object_data.get("csv_file_path")
        s3_object_name = csv_object_data.get("s3_object_name")
        
        try:
            s3_hook.load_file(
                filename=csv_file_path,
                key=s3_object_name,
                bucket_name=BUCKET_NAME,
                replace=True
            )
            print(f"Uploaded CSV to S3: {s3_object_name}")
        except Exception as e:
            print(f"Error uploading CSV to S3: {e}")

    @task
    def update_last_extraction_date(csv_object_data):
        last_extraction_date = csv_object_data.get("last_extraction_date")

        formatted_date = last_extraction_date.strftime('%Y-%m-%d_%H:%M:%S')

        # Actualiza la Variable de Airflow con la nueva fecha de extracción
        Variable.set(LAST_EXTRACTION_VAR, formatted_date)
        print(f"Updated last extraction date to: {formatted_date}")

    # Define task dependencies
    data = extract_data_from_mysql()
    transformed_data = transform_data(data)
    csv_object_data = create_csv(transformed_data)
    print("csv_data", csv_object_data)
    upload_csv_to_s3(csv_object_data)
    update_last_extraction_date(csv_object_data)


mysql_example_dag_dag = mysql_example_dag()
