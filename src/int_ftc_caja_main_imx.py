# Versión del programa
# Version 3.17 - Modificado por Claude 3.5
# 2023-05-25 17:45:00 - Actualizada función listado_movimiento_por_cliente() para mostrar datos en formato de tabla
# 2023-05-25 18:30:00 - Actualizadas funciones registro_movimientos() y listado_movimiento_por_cliente()
# 2023-05-25 19:15:00 - Actualizadas funciones mostrar_lista_usuarios(), mostrar_lista_terceros() y agregada función saldo_total()

import mysql.connector
from mysql.connector import Error
from datetime import datetime
from prettytable import PrettyTable

def connect_to_database():
    """
    Conecta a la base de datos MySQL y devuelve la conexión.
    """
    try:
        connection = mysql.connector.connect(
            host='localhost',
            database='DB_CAJA_FINTECH',
            user='ljgalicia',
            password='Ljgi2831'
        )
        if connection.is_connected():
            return connection
        else:
            raise ConnectionError("No se pudo establecer la conexión a la base de datos.")
    except Error as e:
        raise ConnectionError(f"Error al conectar a la base de datos: {e}")

def close_database(connection):
    """
    Cierra la conexión a la base de datos.
    """
    if connection and connection.is_connected():
        try:
            connection.close()
        except Error as e:
            print(f"Error al cerrar la conexión: {e}")

def execute_query(connection, query, data=None):
    """
    Ejecuta una consulta de modificación (INSERT, UPDATE, DELETE) en la base de datos.
    """
    cursor = connection.cursor()
    try:
        if data:
            cursor.execute(query, data)
        else:
            cursor.execute(query)
        connection.commit()
    except Error as e:
        connection.rollback()
        raise DatabaseError(f"Error al ejecutar la consulta: {e}")
    finally:
        cursor.close()

def fetch_query(connection, query, data=None):
    """
    Ejecuta una consulta de selección (SELECT) en la base de datos y devuelve los resultados.
    """
    cursor = connection.cursor(dictionary=True)
    try:
        if data:
            cursor.execute(query, data)
        else:
            cursor.execute(query)
        return cursor.fetchall()
    except Error as e:
        raise DatabaseError(f"Error al ejecutar la consulta: {e}")
    finally:
        cursor.close()

def validate_user(username, password):
    """
    Valida las credenciales del usuario en la base de datos.
    """
    try:
        connection = connect_to_database()
        query = "SELECT * FROM CAT_USUARIOS WHERE COD_USUARIO = %s AND COD_PASS = %s AND MCA_INHABILITADO = 'N'"
        data = (username, password)
        result = fetch_query(connection, query, data)
        return True if result else False
    except (ConnectionError, DatabaseError) as e:
        print(f"Error durante la validación del usuario: {e}")
        return False
    finally:
        close_database(connection)

def login():
    """
    Maneja el proceso de inicio de sesión del usuario.
    """
    print("Inicio de sesión")
    username = input("Código de usuario: ")
    password = input("Contraseña: ")
    
    try:
        if validate_user(username, password):
            print("Inicio de sesión exitoso.")
            menu_principal(username)
        else:
            print("Usuario o contraseña incorrecto.")
    except Exception as e:
        print(f"Error inesperado durante el inicio de sesión: {e}")

def menu_principal(username):
    """
    Muestra el menú principal y permite al usuario seleccionar una opción.
    """
    while True:
        try:
            print("\nMenu Principal:")
            print("1 - Menu Administrador del sistema.")
            print("2 - Registro de movimientos.")
            print("3 - Listado de movimiento por Cliente.")
            print("4 - Modificar registro de movimientos.")
            print("5 - Saldo total.")
            print("6 - Salir del sistema")
            option = input("Seleccione una opción: ")
            
            if option == '1':
                menu_administrador(username)
            elif option == '2':
                registro_movimientos(username)
            elif option == '3':
                listado_movimiento_por_cliente(username)
            elif option == '4':
                modificar_movimiento(username)
            elif option == '5':
                saldo_total(username)
            elif option == '6':
                print("Saliendo del sistema...")
                break
            else:
                print("Opción no válida. Intente nuevamente.")
        except Exception as e:
            print(f"Error inesperado en el menú principal: {e}")

def menu_administrador(username):
    """
    Muestra el menú del administrador del sistema y permite al usuario seleccionar una opción.
    """
    while True:
        try:
            print("\nMenu Administrador del sistema:")
            print("1.1 - Alta de usuarios.")
            print("1.2 - Alta terceros.")
            print("1.3 - Alta de Tipos de movimientos.")
            print("1.4 - Alta Tipos de terceros.")
            print("1.5 - Alta de Acciones.")
            print("1.6 - Asignar permisos a usuarios.")
            print("1.7 - Mostrar lista de usuarios.")
            print("1.8 - Mostrar lista de terceros.")
            print("1.9 - Alta de permisos a usuarios.")
            print("1.10 - Generar respaldo de datos.")
            print("1.11 - Cargar datos desde archivo.")
            print("1.12 - Regresar al menú principal.")
            option = input("Seleccione una opción: ")
            
            if option == '1.1':
                alta_usuarios(username)
            elif option == '1.2':
                alta_terceros(username)
            elif option == '1.3':
                alta_tipo_movimientos(username)
            elif option == '1.4':
                alta_tipo_terceros(username)
            elif option == '1.5':
                alta_acciones(username)
            elif option == '1.6':
                asignar_permisos(username)
            elif option == '1.7':
                mostrar_lista_usuarios(username)
            elif option == '1.8':
                mostrar_lista_terceros(username)
            elif option == '1.9':
                alta_permisos_usuario(username)
            elif option == '1.10':
                generar_respaldo_datos(username)
            elif option == '1.11':
                cargar_datos_desde_archivo(username)
            elif option == '1.12':
                break
            else:
                print("Opción no válida. Intente nuevamente.")
        except Exception as e:
            print(f"Error inesperado en el menú de administrador: {e}")

def alta_usuarios(username):
    """
    Permite al administrador registrar nuevos usuarios en la base de datos.
    """
    connection = None
    try:
        connection = connect_to_database()
        while True:
            cod_usuario = input("Código de usuario: ")
            tip_usuario = input("Tipo de usuario (ADM, ACC, PRE): ")
            cod_pass = input("Contraseña del usuario: ")
            mca_inhabilitado = input("Marca de inhabilitado (S/N): ")
            
            query = """
            INSERT INTO CAT_USUARIOS (COD_USUARIO, TIP_USUARIO, COD_PASS, FEC_ACTUALIZACION, MCA_INHABILITADO)
            VALUES (%s, %s, %s, %s, %s)
            """
            fec_actualizacion = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            data = (cod_usuario, tip_usuario, cod_pass, fec_actualizacion, mca_inhabilitado)
            execute_query(connection, query, data)
            
            more = input("¿Desea ingresar otro usuario? (S/N): ").upper()
            if more != 'S':
                break

        print("Datos ingresados correctamente.")
    except (ConnectionError, DatabaseError) as e:
        print(f"Error al dar de alta usuarios: {e}")
    finally:
        close_database(connection)

def alta_terceros(username):
    """
    Permite al administrador registrar nuevos terceros en la base de datos.
    """
    connection = None
    try:
        connection = connect_to_database()
        while True:
            id_tercero = input("ID Tercero: ")
            tip_tercero = input("Tipo de tercero (1-4): ")
            nom_tercero = input("Nombre del tercero: ")
            ape_paterno = input("Apellido paterno: ")
            ape_materno = input("Apellido materno: ")
            mca_inhabilitado = input("Marca de inhabilitado (S/N): ")
            
            query = """
            INSERT INTO CAT_TERCEROS (ID_TERCERO, TIP_TERCERO, NOM_TERCERO, APE_PATERNO, APE_MATERNO, FEC_ACTUALIZACION, MCA_INHABILITADO, COD_USUARIO)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            fec_actualizacion = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            data = (id_tercero, tip_tercero, nom_tercero, ape_paterno, ape_materno, fec_actualizacion, mca_inhabilitado, username)
            execute_query(connection, query, data)
            
            more = input("¿Desea ingresar otro tercero? (S/N): ").upper()
            if more != 'S':
                break

        print("Datos ingresados correctamente.")
    except (ConnectionError, DatabaseError) as e:
        print(f"Error al dar de alta terceros: {e}")
    finally:
        close_database(connection)

def alta_tipo_movimientos(username):
    """
    Permite al administrador registrar nuevos tipos de movimientos en la base de datos.
    """
    connection = None
    try:
        connection = connect_to_database()
        while True:
            cod_movimiento = input("Código de movimiento: ")
            desc_movimiento = input("Descripción del movimiento: ")
            tip_movimiento = input("Tipo de movimiento (R/D): ")
            mca_inhabilitado = input("Marca de inhabilitado (S/N): ")
            
            query = """
            INSERT INTO CAT_TIP_MOVIMIENTOS (COD_MOVIMIENTO, DESC_MOVIMIENTO, TIP_MOVIMIENTO, FEC_ACTUALIZACION, MCA_INHABILITADO, COD_USUARIO)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            fec_actualizacion = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            data = (cod_movimiento, desc_movimiento, tip_movimiento, fec_actualizacion, mca_inhabilitado, username)
            execute_query(connection, query, data)
            
            more = input("¿Desea ingresar otro tipo de movimiento? (S/N): ").upper()
            if more != 'S':
                break

        print("Datos ingresados correctamente.")
    except (ConnectionError, DatabaseError) as e:
        print(f"Error al dar de alta tipos de movimientos: {e}")
    finally:
        close_database(connection)

def alta_tipo_terceros(username):
    """
    Permite al administrador registrar nuevos tipos de terceros en la base de datos.
    """
    connection = None
    try:
        connection = connect_to_database()
        while True:
            tip_tercero = input("Tipo de tercero (1-4): ")
            desc_tip_tercero = input("Descripción del tipo de tercero: ")
            mca_inhabilitado = input("Marca de inhabilitado (S/N): ")
            
            query = """
            INSERT INTO CAT_TIP_TERCEROS (TIP_TERCERO, DESC_TIP_TERCERO, FEC_ACTUALIZACION, MCA_INHABILITADO, COD_USUARIO)
            VALUES (%s, %s, %s, %s, %s)
            """
            fec_actualizacion = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            data = (tip_tercero, desc_tip_tercero, fec_actualizacion, mca_inhabilitado, username)
            execute_query(connection, query, data)
            
            more = input("¿Desea ingresar otro tipo de tercero? (S/N): ").upper()
            if more != 'S':
                break

        print("Datos ingresados correctamente.")
    except (ConnectionError, DatabaseError) as e:
        print(f"Error al dar de alta tipos de terceros: {e}")
    finally:
        close_database(connection)

def alta_acciones(username):
    """
    Permite al administrador registrar nuevas acciones en la base de datos.
    """
    connection = None
    try:
        connection = connect_to_database()
        while True:
            id_tercero = input("ID Tercero: ")
            num_acciones = input("Número de acciones: ")
            fec_vigencia = input("Fecha de vigencia (YYYY-MM-DD HH:MM:SS): ")
            mca_inhabilitado = input("Marca de inhabilitado (S/N): ")
            
            query = """
            INSERT INTO CAT_NUM_ACCIONES (ID_TERCERO, NUM_ACCIONES, FEC_VIGENCIA, FEC_ACTUALIZACION, MCA_INHABILITADO, COD_USUARIO)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            fec_actualizacion = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            data = (id_tercero, num_acciones, fec_vigencia, fec_actualizacion, mca_inhabilitado, username)
            execute_query(connection, query, data)
            
            more = input("¿Desea ingresar otra acción? (S/N): ").upper()
            if more != 'S':
                break

        print("Datos ingresados correctamente.")
    except (ConnectionError, DatabaseError) as e:
        print(f"Error al dar de alta acciones: {e}")
    finally:
        close_database(connection)

def asignar_permisos(username):
    """
    Permite al administrador asignar permisos a usuarios en la base de datos.
    """
    connection = None
    try:
        connection = connect_to_database()
        while True:
            cod_usuario = input("Código de usuario: ")
            id_tercero = input("ID Tercero: ")
            tip_tercero = input("Tipo de tercero: ")
            cod_permisos = input("Código de permisos: ")
            
            query = """
            INSERT INTO CAT_PER_USUARIO (COD_USUARIO, ID_TERCERO, TIP_TERCERO, COD_PERMISOS, FEC_ACTUALIZACION, MCA_INHABILITADO)
            VALUES (%s, %s, %s, %s, %s, 'N')
            """
            fec_actualizacion = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            data = (cod_usuario, id_tercero, tip_tercero, cod_permisos, fec_actualizacion)
            execute_query(connection, query, data)
            
            more = input("¿Desea asignar otro permiso? (S/N): ").upper()
            if more != 'S':
                break

        print("Permisos asignados correctamente.")
    except (ConnectionError, DatabaseError) as e:
        print(f"Error al asignar permisos: {e}")
    finally:
        close_database(connection)


def mostrar_lista_usuarios(username):
    """
    Muestra la lista de todos los usuarios registrados en la base de datos en formato de tabla.
    """
    connection = None
    try:
        connection = connect_to_database()
        query = "SELECT * FROM CAT_USUARIOS"
        usuarios = fetch_query(connection, query)
        
        if usuarios:
            table = PrettyTable()
            table.field_names = ["Código de Usuario", "Tipo de Usuario", "Contraseña", "Fecha de Actualización", "Inhabilitado"]
            
            for usuario in usuarios:
                table.add_row([
                    usuario['COD_USUARIO'],
                    usuario['TIP_USUARIO'],
                    '*****',  # Por seguridad, no mostramos la contraseña real
                    usuario['FEC_ACTUALIZACION'],
                    usuario['MCA_INHABILITADO']
                ])
            
            print("\nLista de usuarios:")
            print(table)
        else:
            print("No se encontraron usuarios.")
    except (ConnectionError, DatabaseError) as e:
        print(f"Error al mostrar la lista de usuarios: {e}")
    finally:
        close_database(connection)

def mostrar_lista_terceros(username):
    """
    Muestra la lista de todos los terceros registrados en la base de datos en formato de tabla.
    """
    connection = None
    try:
        connection = connect_to_database()
        query = "SELECT * FROM CAT_TERCEROS"
        terceros = fetch_query(connection, query)
        
        if terceros:
            table = PrettyTable()
            table.field_names = ["ID Tercero", "Tipo de Tercero", "Nombre", "Apellido Paterno", "Apellido Materno", "Fecha de Actualización", "Inhabilitado"]
            
            for tercero in terceros:
                table.add_row([
                    tercero['ID_TERCERO'],
                    tercero['TIP_TERCERO'],
                    tercero['NOM_TERCERO'],
                    tercero['APE_PATERNO'],
                    tercero['APE_MATERNO'],
                    tercero['FEC_ACTUALIZACION'],
                    tercero['MCA_INHABILITADO']
                ])
            
            print("\nLista de terceros:")
            print(table)
        else:
            print("No se encontraron terceros.")
    except (ConnectionError, DatabaseError) as e:
        print(f"Error al mostrar la lista de terceros: {e}")
    finally:
        close_database(connection)

def saldo_total(username):
    """
    Muestra el saldo total de retiros, depósitos y la suma total en formato de tabla.
    """
    connection = None
    try:
        connection = connect_to_database()
        query = """
        SELECT 
            SUM(IMP_RETIRO) as TOTAL_RETIRO, 
            SUM(IMP_DEPOSITO) as TOTAL_DEPOSITO
        FROM HIS_MOVIMIENTOS
        """
        resultado = fetch_query(connection, query)
        
        if resultado and resultado[0]['TOTAL_RETIRO'] is not None and resultado[0]['TOTAL_DEPOSITO'] is not None:
            total_retiro = float(resultado[0]['TOTAL_RETIRO'])
            total_deposito = float(resultado[0]['TOTAL_DEPOSITO'])
            saldo_total = total_deposito - total_retiro
            
            table = PrettyTable()
            table.field_names = ["Total Retiros", "Total Depósitos", "Saldo Total"]
            table.add_row([
                f"{total_retiro:,.2f}",
                f"{total_deposito:,.2f}",
                f"{saldo_total:,.2f}"
            ])
            
            print("\nSaldo Total:")
            print(table)
        else:
            print("No se encontraron movimientos.")
    except (ConnectionError, DatabaseError) as e:
        print(f"Error al calcular el saldo total: {e}")
    finally:
        close_database(connection)


def alta_permisos_usuario(username):
    """
    Permite al administrador asignar permisos a usuarios en la base de datos.
    """
    connection = None
    try:
        connection = connect_to_database()
        while True:
            cod_usuario = input("Código de usuario: ")
            permisos = input("Permisos (separados por coma): ").split(',')
            
            for permiso in permisos:
                query = """
                INSERT INTO CAT_PER_USUARIO (COD_USUARIO, ID_TERCERO, TIP_TERCERO, COD_PERMISOS, FEC_ACTUALIZACION, MCA_INHABILITADO)
                VALUES (%s, NULL, NULL, %s, %s, 'N')
                """
                fec_actualizacion = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                data = (cod_usuario, permiso.strip(), fec_actualizacion)
                execute_query(connection, query, data)
            
            more = input("¿Desea ingresar más permisos? (S/N): ").upper()
            if more != 'S':
                break

        print("Permisos asignados correctamente.")
    except (ConnectionError, DatabaseError) as e:
        print(f"Error al asignar permisos: {e}")
    finally:
        close_database(connection)

def registro_movimientos(username):
    """
    Permite al administrador registrar nuevos movimientos en la base de datos.
    """
    connection = None
    try:
        connection = connect_to_database()
        while True:
            id_tercero = input("ID Tercero: ")
            tip_tercero = input("Tipo de tercero: ")
            
            # Mostrar lista de tipos de movimientos
            print("\nTipos de movimientos disponibles:")
            query_tipos = "SELECT COD_MOVIMIENTO, DESC_MOVIMIENTO, TIP_MOVIMIENTO FROM CAT_TIP_MOVIMIENTOS"
            tipos_movimientos = fetch_query(connection, query_tipos)
            
            table_tipos = PrettyTable()
            table_tipos.field_names = ["Código", "Descripción", "Tipo"]
            for tipo in tipos_movimientos:
                table_tipos.add_row([tipo['COD_MOVIMIENTO'], tipo['DESC_MOVIMIENTO'], tipo['TIP_MOVIMIENTO']])
            print(table_tipos)
            
            cod_movimiento = input("Código de movimiento: ")
            
            # Solicitar fecha en formato DD-MM-YYYY
            while True:
                fec_registro_str = input("Fecha de registro (DD-MM-YYYY): ")
                try:
                    fec_registro = datetime.strptime(fec_registro_str, '%d-%m-%Y')
                    break
                except ValueError:
                    print("Formato de fecha incorrecto. Use DD-MM-YYYY.")
            
            imp_retiro = input("Importe de retiro (puede dejarlo en blanco si no aplica): ")
            imp_deposito = input("Importe de depósito (puede dejarlo en blanco si no aplica): ")
            
            # Convertir campos vacíos a None
            imp_retiro = None if imp_retiro.strip() == '' else imp_retiro
            imp_deposito = None if imp_deposito.strip() == '' else imp_deposito
            
            query = """
            INSERT INTO HIS_MOVIMIENTOS (ID_TERCERO, TIP_TERCERO, COD_MOVIMIENTO, FEC_REGISTRO, IMP_RETIRO, IMP_DEPOSITO, FEC_ACTUALIZACION, MCA_INHABILITADO, COD_USUARIO)
            VALUES (%s, %s, %s, %s, %s, %s, %s, 'N', %s)
            """
            fec_actualizacion = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            data = (id_tercero, tip_tercero, cod_movimiento, fec_registro.strftime('%Y-%m-%d'), imp_retiro, imp_deposito, fec_actualizacion, username)
            execute_query(connection, query, data)
            
            more = input("¿Desea ingresar otro movimiento? (S/N): ").upper()
            if more != 'S':
                break

        print("Datos ingresados correctamente.")
    except (ConnectionError, DatabaseError) as e:
        print(f"Error al registrar movimientos: {e}")
    finally:
        close_database(connection)

def listado_movimiento_por_cliente(username):
    """
    Muestra un listado de movimientos por cliente en formato de tabla.
    """
    connection = None
    try:
        connection = connect_to_database()
        id_tercero = input("Ingrese el ID del tercero: ")
        tip_tercero = input("Ingrese el tipo del tercero: ")
        
        query = """
        SELECT HM.ID_TERCERO, TT.DESC_TIP_TERCERO, HM.FEC_REGISTRO, TM.DESC_MOVIMIENTO, 
               HM.IMP_RETIRO, HM.IMP_DEPOSITO, HM.FEC_ACTUALIZACION
        FROM HIS_MOVIMIENTOS HM
        JOIN CAT_TIP_TERCEROS TT ON HM.TIP_TERCERO = TT.TIP_TERCERO
        JOIN CAT_TIP_MOVIMIENTOS TM ON HM.COD_MOVIMIENTO = TM.COD_MOVIMIENTO
        WHERE HM.ID_TERCERO = %s AND HM.TIP_TERCERO = %s
        ORDER BY HM.FEC_REGISTRO
        """
        data = (id_tercero, tip_tercero)
        movimientos = fetch_query(connection, query, data)
        
        if movimientos:
            table = PrettyTable()
            table.field_names = ["ID Tercero", "Tipo Tercero", "Fecha Registro", "Tipo Movimiento", 
                                 "Importe Retiro", "Importe Depósito", "Fecha Actualización"]
            
            suma_depositos = 0
            for movimiento in movimientos:
                table.add_row([
                    movimiento['ID_TERCERO'],
                    movimiento['DESC_TIP_TERCERO'],
                    movimiento['FEC_REGISTRO'].strftime('%Y-%m-%d') if isinstance(movimiento['FEC_REGISTRO'], datetime) else movimiento['FEC_REGISTRO'],
                    movimiento['DESC_MOVIMIENTO'],
                    movimiento['IMP_RETIRO'],
                    movimiento['IMP_DEPOSITO'],
                    movimiento['FEC_ACTUALIZACION'].strftime('%Y-%m-%d %H:%M:%S') if isinstance(movimiento['FEC_ACTUALIZACION'], datetime) else movimiento['FEC_ACTUALIZACION']
                ])
                if movimiento['IMP_DEPOSITO']:
                    suma_depositos += float(movimiento['IMP_DEPOSITO'])
            
            print("\nListado de movimientos por cliente:")
            print(table)
            print(f"\nSuma total de depósitos: {suma_depositos:.2f}")
        else:
            print("No se encontraron movimientos para este cliente.")
    except (ConnectionError, DatabaseError) as e:
        print(f"Error al listar movimientos por cliente: {e}")
    finally:
        close_database(connection)

def generar_respaldo_datos(username):
    """
    Genera un archivo de texto con la información de las tablas de la base de datos.
    """
    connection = None
    try:
        connection = connect_to_database()
        tablas = [
            'CAT_USUARIOS',
            'CAT_TERCEROS',
            'CAT_TIP_MOVIMIENTOS',
            'CAT_TIP_TERCEROS',
            'CAT_NUM_ACCIONES',
            'CAT_PER_USUARIO',
            'HIS_MOVIMIENTOS'
        ]
        
        for tabla in tablas:
            query = f"SELECT * FROM {tabla}"
            datos = fetch_query(connection, query)
            
            if datos:
                with open(f"{tabla}.txt", 'w') as file:
                    for registro in datos:
                        file.write(','.join([str(registro[col]) if registro[col] is not None else 'NULL' for col in registro]) + '\n')
            
            print(f"Respaldo de datos para {tabla} generado en {tabla}.txt")
    except (ConnectionError, DatabaseError) as e:
        print(f"Error al generar respaldo de datos: {e}")
    finally:
        close_database(connection)

def cargar_datos_desde_archivo(username):
    """
    Carga datos desde un archivo de texto a las tablas correspondientes en la base de datos.
    El nombre del archivo (sin la extensión .txt) se utiliza para determinar la tabla en la que se deben insertar los datos.
    """
    connection = None
    try:
        connection = connect_to_database()
        archivo = input("Ingrese el nombre del archivo para cargar los datos (sin la extensión .txt): ")
        archivo_completo = archivo + ".txt"
        
        with open(archivo_completo, 'r') as file:
            lineas = file.readlines()
        
        # Extraer el nombre de la tabla del archivo (sin la extensión)
        tabla = archivo
        registros = []
        
        for linea in lineas:
            datos = linea.strip().split(',')
            registros.append(datos)
        
        if tabla == 'CAT_TIP_TERCEROS':
            query = """
            INSERT INTO CAT_TIP_TERCEROS (TIP_TERCERO, DESC_TIP_TERCERO, FEC_ACTUALIZACION, MCA_INHABILITADO, COD_USUARIO)
            VALUES (%s, %s, %s, %s, %s)
            """
            for registro in registros:
                # Validar que MCA_INHABILITADO solo acepte 'S' o 'N'
                if registro[2] not in ['S', 'N']:
                    print(f"Error: Valor inválido para MCA_INHABILITADO en el registro {registro}")
                    continue
                
                data = (registro[0], registro[1], registro[2], registro[3], registro[4])
                execute_query(connection, query, data)
        else:
            print(f"Tabla {tabla} no reconocida o no soportada.")
        
        print(f"Datos cargados desde el archivo {archivo_completo}.")
    except FileNotFoundError:
        print(f"El archivo {archivo_completo} no se encontró.")
    except (ConnectionError, DatabaseError) as e:
        print(f"Error al cargar los datos: {e}")
    finally:
        close_database(connection)

def modificar_movimiento(username):
    """
    Permite modificar un registro existente en la tabla HIS_MOVIMIENTOS.
    """
    connection = None
    try:
        connection = connect_to_database()
        
        # Solicitar los datos de la clave primaria
        id_tercero = input("Ingrese el ID del tercero: ")
        tip_tercero = input("Ingrese el tipo de tercero: ")
        fec_registro_original = input("Ingrese la fecha de registro original (YYYY-MM-DD HH:MM:SS): ")
        fec_actualizacion = input("Ingrese la fecha de actualización original (YYYY-MM-DD HH:MM:SS): ")
        
        # Buscar el registro
        query = """
        SELECT * FROM HIS_MOVIMIENTOS
        WHERE ID_TERCERO = %s AND TIP_TERCERO = %s AND FEC_REGISTRO = %s AND FEC_ACTUALIZACION = %s
        """
        data = (id_tercero, tip_tercero, fec_registro_original, fec_actualizacion)
        resultado = fetch_query(connection, query, data)
        
        if not resultado:
            print("No se encontró el registro especificado.")
            return
        
        registro = resultado[0]
        
        # Solicitar nuevo valor para FEC_REGISTRO
        print("\nDeje en blanco si no desea modificar la fecha de registro.")
        nueva_fec_registro = input(f"Nueva fecha de registro [{registro['FEC_REGISTRO']}]: ") or registro['FEC_REGISTRO']
        
        # Solicitar nuevos valores para los campos modificables
        print("\nDeje en blanco los campos que no desea modificar.")
        nuevo_cod_movimiento = input(f"Nuevo código de movimiento [{registro['COD_MOVIMIENTO']}]: ") or registro['COD_MOVIMIENTO']
        nuevo_imp_retiro = input(f"Nuevo importe de retiro [{registro['IMP_RETIRO']}]: ") or registro['IMP_RETIRO']
        nuevo_imp_deposito = input(f"Nuevo importe de depósito [{registro['IMP_DEPOSITO']}]: ") or registro['IMP_DEPOSITO']
        nueva_mca_inhabilitado = input(f"Nueva marca de inhabilitado (S/N) [{registro['MCA_INHABILITADO']}]: ") or registro['MCA_INHABILITADO']
        
        # Actualizar el registro
        query = """
        UPDATE HIS_MOVIMIENTOS
        SET FEC_REGISTRO = %s, COD_MOVIMIENTO = %s, IMP_RETIRO = %s, IMP_DEPOSITO = %s, MCA_INHABILITADO = %s, FEC_ACTUALIZACION = %s, COD_USUARIO = %s
        WHERE ID_TERCERO = %s AND TIP_TERCERO = %s AND FEC_REGISTRO = %s AND FEC_ACTUALIZACION = %s
        """
        nueva_fec_actualizacion = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        data = (nueva_fec_registro, nuevo_cod_movimiento, nuevo_imp_retiro, nuevo_imp_deposito, nueva_mca_inhabilitado, 
                nueva_fec_actualizacion, username, id_tercero, tip_tercero, fec_registro_original, fec_actualizacion)
        
        execute_query(connection, query, data)
        print("Registro actualizado correctamente.")
        
    except (ConnectionError, DatabaseError) as e:
        print(f"Error al modificar el movimiento: {e}")
    finally:
        close_database(connection)

class DatabaseError(Exception):
    """Excepción personalizada para errores de base de datos."""
    pass

if __name__ == "__main__":
    try:
        login()
    except Exception as e:
        print(f"Error crítico del sistema: {e}")
        print("El programa se cerrará.")

# Fin del programa
