from faker import Faker
import mysql.connector
import random
import datetime
from decimal import Decimal
import json

def lambda_handler(event, context):
    
    # Configura Faker
    fake = Faker()

    CANTIDAD_COMPRAS = 20
    CANTIDAD_VENTAS = CANTIDAD_COMPRAS * 1000

    # Conectar a la base de datos MySQL
    connection = mysql.connector.connect(
        host='fake-database-grupo3-1.cfqay6u8sikg.us-east-1.rds.amazonaws.com',  # Nombre del servicio definido en Docker Compose
        user='admin',
        password='awsrdsgruop3fakerlaboratorio',
        database='bd-grupo-3-v2'
    )

    cursor = connection.cursor()

    ### INICIO PRIMERA EJECUCIÓN

    # Crear tabla de proveedores si no existe
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Proveedor (
            id_proveedor INT AUTO_INCREMENT PRIMARY KEY,
            tipo_documento VARCHAR(50) NOT NULL,
            nro_documento VARCHAR(50) NOT NULL,
            razon_social VARCHAR(255),
            nombre VARCHAR(255),
            apellido_pa VARCHAR(255),
            apellido_ma VARCHAR(255),
            banco VARCHAR(255),
            cuenta VARCHAR(255),
            cci VARCHAR(255),
            direccion VARCHAR(255),
            fecha_extraccion DATETIME DEFAULT CURRENT_TIMESTAMP
        );
    ''')

    # Verificar si la tabla Proveedor está vacía
    cursor.execute("SELECT COUNT(*) FROM Proveedor;")
    proveedores_count = cursor.fetchone()[0]

    # Insertar proveedores si la tabla está vacía
    #if proveedores_count == 0:
    #    for _ in range(1000):
    cursor.execute('''
        INSERT INTO Proveedor (tipo_documento, nro_documento, razon_social, nombre, apellido_pa, apellido_ma, banco, cuenta, cci, direccion)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ''', (
        fake.random_element(elements=('DNI', 'RUC')),
        fake.unique.random_number(digits=8),
        fake.company(),
        fake.first_name(),
        fake.last_name(),
        fake.last_name(),
        fake.bank_country(),
        fake.iban(),
        fake.iban(),
        fake.address()
    ))
    print("Se insertaron proveedores.")
    #else:
    #    print("La tabla Proveedor ya tiene datos.")

    # Crear tabla de productos si no existe
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Producto (
            cod_producto INT AUTO_INCREMENT PRIMARY KEY,
            unidad_medida VARCHAR(50),
            tipo_moneda VARCHAR(10),
            costo_promedio_unitario DECIMAL(15,2),
            precio_unitario DECIMAL(12,2),
            stock_minimo INT,
            stock_maximo INT,
            fecha_extraccion DATETIME DEFAULT CURRENT_TIMESTAMP
        );
    ''')

    # Verificar si la tabla Producto está vacía
    cursor.execute("SELECT COUNT(*) FROM Producto;")
    productos_count = cursor.fetchone()[0]

    # Insertar 1000 productos si la tabla está vacía
    if productos_count == 0:
        for _ in range(3000):
            precio_unitario = fake.pydecimal(left_digits=4, right_digits=2, positive=True)
            costo_promedio_unitario = fake.pydecimal(left_digits=4, right_digits=2, positive=True, max_value=float(precio_unitario) * 0.8)
            cursor.execute('''
                INSERT INTO Producto (unidad_medida, tipo_moneda, costo_promedio_unitario, precio_unitario, stock_minimo, stock_maximo)
                VALUES (%s, %s, %s, %s, %s, %s)
            ''', (
                'UND',
                'PEN',
                costo_promedio_unitario,
                precio_unitario,
                fake.random_int(min=100, max=200),
                fake.random_int(min=10000, max=100000)
            ))
        print("Se insertaron productos.")
    else:
        print("La tabla Producto ya tiene datos.")

    # Crear tabla de clientes si no existe
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Cliente (
            id_cliente INT AUTO_INCREMENT PRIMARY KEY,
            nombre VARCHAR(255) NOT NULL,
            apellido_pa VARCHAR(255) NOT NULL,
            apellido_ma VARCHAR(255) NOT NULL,
            direccion VARCHAR(255),
            tipo_documento VARCHAR(50),
            nro_documento VARCHAR(50),
            correo VARCHAR(255),
            fecha_extraccion DATETIME DEFAULT CURRENT_TIMESTAMP
        );
    ''')

    # Verificar si la tabla Cliente está vacía
    cursor.execute("SELECT COUNT(*) FROM Cliente;")
    clientes_count = cursor.fetchone()[0]

    # Insertar 2000 clientes si la tabla está vacía
    if clientes_count == 0:
        for _ in range(10000):
            cursor.execute('''
                INSERT INTO Cliente (nombre, apellido_pa, apellido_ma, direccion, tipo_documento, nro_documento, correo)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            ''', (
                fake.first_name(),
                fake.last_name(),
                fake.last_name(),
                fake.address(),
                fake.random_element(elements=('DNI', 'RUC', 'PASAPORTE')),
                fake.unique.random_number(digits=8),
                fake.email()
            ))
        print("Se insertaron clientes.")
    else:
        print("La tabla Cliente ya tiene datos.")

    # Crear tabla de stock de productos si no existe incluyendo un campo con la fecha de la ultima actualizacion
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Stock (
            id_stock INT AUTO_INCREMENT PRIMARY KEY,
            id_producto INT,
            stock_actual INT,
            stock_valorizado DECIMAL(15,2),
            fecha_actualizacion DATE,
            FOREIGN KEY (id_producto) REFERENCES Producto(cod_producto),
            fecha_extraccion DATETIME DEFAULT CURRENT_TIMESTAMP
        );
    ''')

    # Verificar si la tabla Stock está vacía
    cursor.execute("SELECT COUNT(*) FROM Stock;")
    stocks_count = cursor.fetchone()[0]

    # Crear stock inicial de cada producto
    if stocks_count == 0:
        cursor.execute("SELECT cod_producto, precio_unitario FROM Producto;")
        productos = cursor.fetchall()
        for producto in productos:
            cod_producto, precio_unitario = producto
            stock = fake.random_int(min=0, max=100)
            stock_valorizado = float(precio_unitario) * stock
            cursor.execute('''
                INSERT INTO Stock (id_producto, stock_actual, stock_valorizado, fecha_actualizacion)
                VALUES (%s, %s, %s, %s)
            ''', (
                cod_producto,
                stock,
                stock_valorizado,
                datetime.datetime.now().date()
            ))
        print("Se insertó el stock inicial de los productos.")
    else:
        print("La tabla Stock ya tiene datos.")

    # Crear tabla de compras si no existe
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Compra (
            id_compra INT AUTO_INCREMENT PRIMARY KEY,
            id_proveedor INT,
            cod_producto INT,
            cantidad_compra INT,
            costo_promedio_unitario DECIMAL(15,2),
            monto_compra DECIMAL(15,2),
            fecha_hora DATETIME,
            FOREIGN KEY (id_proveedor) REFERENCES Proveedor(id_proveedor),
            FOREIGN KEY (cod_producto) REFERENCES Producto(cod_producto),
            fecha_extraccion DATETIME DEFAULT CURRENT_TIMESTAMP
        );
    ''')

    # Crear tabla de ventas si no existe
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Venta (
            id_venta INT AUTO_INCREMENT PRIMARY KEY,
            id_cliente INT,
            cod_producto INT,
            cantidad_venta INT,
            precio_unitario DECIMAL(12,2),
            monto_venta DECIMAL(15,2),
            fecha_hora DATETIME,
            FOREIGN KEY (id_cliente) REFERENCES Cliente(id_cliente),
            FOREIGN KEY (cod_producto) REFERENCES Producto(cod_producto),
            fecha_extraccion DATETIME DEFAULT CURRENT_TIMESTAMP 
        );
    ''')

    # Crear tabla de movimientos de productos si no existe
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Movimiento_producto (
            id_movimiento INT AUTO_INCREMENT PRIMARY KEY,
            fecha_hora DATETIME,
            cod_producto INT,
            tipo_moneda VARCHAR(10) DEFAULT 'PEN',
            cantidad INT,
            id_compra INT,
            id_venta INT,
            costo_promedio_unitario DECIMAL(15,2),
            precio_unitario DECIMAL(12,2),
            clase VARCHAR(50),
            FOREIGN KEY (cod_producto) REFERENCES Producto(cod_producto),
            FOREIGN KEY (id_compra) REFERENCES Compra(id_compra),
            FOREIGN KEY (id_venta) REFERENCES Venta(id_venta),
            fecha_extraccion DATETIME DEFAULT CURRENT_TIMESTAMP 
        );
    ''')

    # Insertar compras y actualizar stock
    for _ in range(CANTIDAD_COMPRAS):
        cursor.execute("SELECT id_proveedor FROM Proveedor ORDER BY RAND() LIMIT 1;")
        id_proveedor = cursor.fetchone()[0]
        cursor.execute("SELECT cod_producto, costo_promedio_unitario FROM Producto ORDER BY RAND() LIMIT 1;")
        cod_producto, costo_promedio = cursor.fetchone()
        cantidad_compra = fake.random_int(min=100, max=500)
        costo_promedio_unitario = round(Decimal(costo_promedio) * Decimal(random.uniform(0.98, 1.02)), 2)
        monto_compra = costo_promedio_unitario * cantidad_compra
        fecha_hora = datetime.datetime.now()

        cursor.execute('''
            INSERT INTO Compra (id_proveedor, cod_producto, cantidad_compra, costo_promedio_unitario, monto_compra, fecha_hora)
            VALUES (%s, %s, %s, %s, %s, %s)
        ''', (
            id_proveedor,
            cod_producto,
            cantidad_compra,
            costo_promedio_unitario,
            monto_compra,
            fecha_hora
        ))
        id_compra = cursor.lastrowid

        # Actualizar el stock del producto
        cursor.execute("UPDATE Stock SET stock_actual = stock_actual + %s, stock_valorizado = stock_valorizado + %s WHERE id_producto = %s;",
                    (cantidad_compra, monto_compra, cod_producto))

        # Insertar movimiento de producto
        cursor.execute('''
            INSERT INTO Movimiento_producto (fecha_hora, cod_producto, cantidad, id_compra, costo_promedio_unitario, clase)
            VALUES (%s, %s, %s, %s, %s, %s)
        ''', (
            fecha_hora,
            cod_producto,
            cantidad_compra,
            id_compra,
            costo_promedio_unitario,
            'Ingreso'
        ))

    # Insertar ventas y actualizar stock
    for _ in range(3000):
        cursor.execute("SELECT id_cliente FROM Cliente ORDER BY RAND() LIMIT 1;")
        id_cliente = cursor.fetchone()[0]
        cursor.execute("SELECT cod_producto, precio_unitario FROM Producto ORDER BY RAND() LIMIT 1;")
        cod_producto, precio_unitario = cursor.fetchone()
        cantidad_venta = fake.random_int(min=1, max=50)
        
        # Verificar el stock actual
        cursor.execute("SELECT stock_actual FROM Stock WHERE id_producto = %s;", (cod_producto,))
        stock_actual = cursor.fetchone()[0]
        
        if stock_actual < cantidad_venta:
            # Realizar una compra para reponer el stock
            cantidad_compra = cantidad_venta * 2
            cursor.execute("SELECT id_proveedor FROM Proveedor ORDER BY RAND() LIMIT 1;")
            id_proveedor = cursor.fetchone()[0]
            costo_promedio_unitario = round(Decimal(precio_unitario) * Decimal(random.uniform(0.98, 1.02)), 2)
            monto_compra = costo_promedio_unitario * cantidad_compra
            fecha_hora = datetime.datetime.now()

            cursor.execute('''
                INSERT INTO Compra (id_proveedor, cod_producto, cantidad_compra, costo_promedio_unitario, monto_compra, fecha_hora)
                VALUES (%s, %s, %s, %s, %s, %s)
            ''', (
                id_proveedor,
                cod_producto,
                cantidad_compra,
                costo_promedio_unitario,
                monto_compra,
                fecha_hora
            ))
            id_compra = cursor.lastrowid

            # Actualizar el stock del producto
            cursor.execute("UPDATE Stock SET stock_actual = stock_actual + %s, stock_valorizado = stock_valorizado + %s WHERE id_producto = %s;",
                        (cantidad_compra, monto_compra, cod_producto))

            # Insertar movimiento de producto
            cursor.execute('''
                INSERT INTO Movimiento_producto (fecha_hora, cod_producto, cantidad, id_compra, costo_promedio_unitario, clase)
                VALUES (%s, %s, %s, %s, %s, %s)
            ''', (
                fecha_hora,
                cod_producto,
                cantidad_compra,
                id_compra,
                costo_promedio_unitario,
                'Ingreso'
            ))

        # Realizar la venta
        monto_venta = precio_unitario * cantidad_venta
        fecha_hora = datetime.datetime.now()

        cursor.execute('''
            INSERT INTO Venta (id_cliente, cod_producto, cantidad_venta, precio_unitario, monto_venta, fecha_hora)
            VALUES (%s, %s, %s, %s, %s, %s)
        ''', (
            id_cliente,
            cod_producto,
            cantidad_venta,
            precio_unitario,
            monto_venta,
            fecha_hora
        ))
        id_venta = cursor.lastrowid

        # Actualizar el stock del producto
        cursor.execute("UPDATE Stock SET stock_actual = stock_actual - %s, stock_valorizado = stock_valorizado - %s WHERE id_producto = %s;",
                    (cantidad_venta, monto_venta, cod_producto))

        # Insertar movimiento de producto
        cursor.execute('''
            INSERT INTO Movimiento_producto (fecha_hora, cod_producto, cantidad, id_venta, precio_unitario, clase)
            VALUES (%s, %s, %s, %s, %s, %s)
        ''', (
            fecha_hora,
            cod_producto,
            cantidad_venta,
            id_venta,
            precio_unitario,
            'Egreso'
        ))

    # Confirmar los cambios en la base de datos
    connection.commit()

    # Cerrar el cursor y la conexión
    cursor.close()
    connection.close()

    
    return {
        'statusCode': 200,
        'body': json.dumps('Done!')
    }
