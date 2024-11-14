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
CSV_FILE_PATH_TEMPLATE = "/tmp/{table_name}.csv"
S3_OBJECT_NAME_TEMPLATE = "landing/customers/{table_name}.csv"

# Reusable function for ETL process
def extract_transform_load(table_name, attributes):
    @task
    def extract_data_from_mysql():
        mysql_hook = MySqlHook(mysql_conn_id='mysql_conn_id')
        query = f"SELECT * FROM `bd-grupo-3`.{table_name}"
        connection = mysql_hook.get_conn()
        cursor = connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        connection.close()
        return rows

    @task
    def transform_data(data):
        transformed_data = []
        for row in data:
            transformed_row = {attributes[i]: row[i] for i in range(len(attributes))}
            transformed_data.append(transformed_row)
        return transformed_data

    @task
    def create_csv(transformed_data):
        csv_file_path = CSV_FILE_PATH_TEMPLATE.format(table_name=table_name)
        try:
            with open(csv_file_path, 'w') as file:
                file.write(",".join(attributes) + "\n")
                for row in transformed_data:
                    file.write(",".join(str(row[attr]) for attr in attributes) + "\n")
            return csv_file_path
        except Exception as e:
            print(f"Error creating CSV file: {e}")

    @task
    def upload_csv_to_s3(csv_file_path):
        s3_hook = S3Hook(aws_conn_id="aws_default")
        s3_object_name = S3_OBJECT_NAME_TEMPLATE.format(table_name=table_name)
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

    data = extract_data_from_mysql()
    transformed_data = transform_data(data)
    csv_file_path = create_csv(transformed_data)
    upload_csv_to_s3(csv_file_path)

# Define the DAG
@dag(dag_id="dag_general",default_args=default_args, schedule_interval=None, start_date=days_ago(1), catchup=False, tags=['mysql_example_general'])
def mysql_example_dag():
    tables = {
        "Cliente": ["id_cliente","apellido_pa","apellido_ma","direccion","tipo_documento","nro_documento","correo"],
        "Compra": ["id_compra", "id_proveedor", "cod_producto", "cantidad_compra", "costo_promedio_unitario", "monto_compra", "fecha_hora"],
        "Producto": ["cod_producto", "unidad_medida", "tipo_moneda", "costo_promedio_unitario", "precio_unitario", "stock_minimo", "stock_maximo"],
        "Stock": ["id_stock", "id_producto", "stock_actual", "stock_valorizado", "fecha_actualizacion"],
        "Proveedor": ["id_proveedor", "tipo_documento", "nro_documento", "razon_social", "nombre", "apellido_pa", "apellido_ma", "banco", "cuenta", "cci", "direccion"],
        "Venta": ["id_venta", "id_cliente", "cod_producto", "cantidad_venta", "precio_unitario", "monto_venta", "fecha_hora"],
        "Movimiento": ["id_movimiento", "fecha_hora", "cod_producto", "tipo_moneda", "cantidad", "id_compra", "id_venta", "costo_promedio_unitario", "precio_unitario", "clase"]
    }

    for table_name, attributes in tables.items():
        extract_transform_load(table_name, attributes)

# Instantiate the DAG
mysql_example_dag_dag = mysql_example_dag()