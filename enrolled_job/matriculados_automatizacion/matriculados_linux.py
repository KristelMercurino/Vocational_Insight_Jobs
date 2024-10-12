# main_optimized.py

import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import subprocess
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import SQLAlchemyError  # Importar excepción específica
import pandas as pd
import tempfile
from dotenv import load_dotenv
import logging

def setup_logging():
    """
    Configura el sistema de logging.
    """
    log_directory = "/home/ubuntu/Vocational_Insight_Jobs/logs"
    log_filename = "enrolled_job_logs.log"
    log_path = os.path.join(log_directory, log_filename)

    # Crear directorio de logs si no existe
    os.makedirs(log_directory, exist_ok=True)

    # Configuración básica de logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_path),
            logging.StreamHandler()  # Opcional: también loguear en consola
        ]
    )

def get_table_columns(engine, table_name):
    """
    Obtiene la lista de columnas existentes en una tabla de la base de datos.
    """
    inspector = inspect(engine)
    try:
        columns_info = inspector.get_columns(table_name)
        table_columns = [column['name'] for column in columns_info]
        logging.info(f"Columnas existentes en la tabla '{table_name}': {table_columns}")
        return table_columns
    except Exception as e:
        error_message = str(e)[:300]
        logging.error(f"Fallo al obtener columnas de la tabla '{table_name}': {error_message}")
        return []

def main():
    # Configurar logging
    setup_logging()
    logging.info("Script main_optimized.py iniciado.")

    try:
        # Cargar variables de entorno desde .env
        load_dotenv()
        logging.info("Variables de entorno cargadas exitosamente.")

        # Configuración de la base de datos
        DB_USER = os.getenv("DB_USER")
        DB_PASS = os.getenv("DB_PASS")
        DB_HOST = os.getenv("DB_HOST")
        DB_PORT = os.getenv("DB_PORT")
        DB_NAME = os.getenv("DB_NAME")
        WINRAR_PATH = os.getenv("WINRAR_PATH", "C:\\Program Files\\WinRAR\\WinRAR.exe")

        # Verificar que todas las variables de entorno necesarias estén presentes
        if not all([DB_USER, DB_PASS, DB_HOST, DB_PORT, DB_NAME]):
            logging.error("Faltan variables de entorno requeridas para la configuración de la base de datos.")
            return

        # Definir el URI de conexión a MariaDB
        DB_URI = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        logging.info("URI de conexión a la base de datos construido.")

        # Crear el engine de la base de datos
        engine = create_engine(DB_URI)
        logging.info("Engine de la base de datos creado exitosamente.")

        # Obtener las columnas existentes en la tabla de destino
        target_table = 'registro_matriculas_1'
        table_columns = get_table_columns(engine, target_table)
        if not table_columns:
            logging.error(f"No se pudo obtener las columnas de la tabla '{target_table}'. Abortando el script.")
            return

        # Diccionario para renombrar columnas
        matriculas_rename = {
            "cat_periodo": "periodo",
            "id": "id_matricula",
            "codigo_unico": "codigo_unico",
            "mrun": "mrun",
            "gen_alu": "gen_alu",
            "fec_nac_alu": "fec_nac_alumno",
            "rango_edad": "rango_edad",
            "anio_ing_carr_ori": "anio_ing_carr_ori",
            "sem_ing_carr_ori": "sem_ing_carr_ori",
            "anio_ing_carr_act": "anio_ing_carr_act",
            "sem_ing_carr_act": "sem_ing_carr_act",
            "tipo_inst_1": "tipo_instituto",
            "tipo_inst_2": "tipo_inst_2",
            "tipo_inst_3": "tipo_inst_3",
            "cod_inst": "cod_institucion",
            "nomb_inst": "institución",
            "cod_sede": "cod_sede",
            "nomb_sede": "nombre_sede",
            "cod_carrera": "cod_carrera",
            "nomb_carrera": "carrera",
            "modalidad": "modalidad",
            "jornada": "jornada",
            "version": "version",
            "tipo_plan_carr": "tipo_plan_carr",
            "dur_estudio_carr": "dur_egreso_carrera",
            "dur_proceso_tit": "dur_titulacion",
            "dur_total_carr": "dur_carrera",
            "region_sede": "region_sede",
            "provincia_sede": "provincia_sede",
            "comuna_sede": "comuna_sede",
            "nivel_global": "grado_academico",
            "nivel_carrera_1": "nivel_carrera_det",
            "nivel_carrera_2": "nivel_carrera",
            "requisito_ingreso": "requisito_ingreso",
            "vigencia_carrera": "vigencia_carrera",
            "formato_valores": "formato_valores",
            "valor_matricula": "valor_matricula",
            "valor_arancel": "valor_mensualidad",
            "codigo_demre": "codigo_demre",
            "area_conocimiento": "area_conocimiento",
            "cine_f_97_area": "area_carrera",
            "cine_f_97_subarea": "subarea_carrera",
            "area_carrera_generica": "area_carrera_generica",
            "cine_f_13_area": "area_profesion",
            "cine_f_13_subarea": "subarea_carrera_2",
            "acreditada_carr": "acreditación_carrera",
            "acreditada_inst": "acreditación_institucion",
            "acre_inst_desde_hasta": "acre_inst_desde_hasta",
            "acre_inst_anio": "año_acreditacion",
            "costo_proceso_titulacion": "costo_p_titulacion",
            "costo_obtencion_titulo_diploma": "costo_diploma",
            "forma_ingreso": "forma_ingreso"
        }

        def extract_data():
            """
            Extrae enlaces de archivos .rar de la página especificada, identifica el año,
            y retorna una lista de diccionarios con la información extraída.
            """
            url = "https://datosabiertos.mineduc.cl/matricula-en-educacion-superior/"
            try:
                response = requests.get(url, timeout=30)
                logging.info(f"Accediendo a la URL: {url}")
                response.raise_for_status()
            except requests.exceptions.RequestException as e:
                logging.error(f"Fallo en la petición HTTP: {e}")
                return []

            data = []

            soup = BeautifulSoup(response.content, "html.parser")
            links = soup.find_all('a', href=True)
            logging.info(f"Se encontraron {len(links)} enlaces en la página.")

            for link in links:
                href = link['href']
                if href.endswith('.rar'):
                    # Extraer el año del nombre del archivo .rar
                    year = ''.join(filter(str.isdigit, href))
                    if len(year) >= 4:
                        year = int(year[:4])  # Tomar los primeros 4 dígitos como año
                        data.append({
                            'year': year,
                            'url': href,
                            'preprocessed_at': datetime.now()
                        })

            logging.info(f"Se encontraron {len(data)} archivos .rar para procesar.")
            return data

        # Extraer datos
        data = extract_data()

        if not data:
            logging.warning("No se encontraron archivos .rar para procesar.")
            return

        # Procesar los archivos .rar uno por uno para minimizar el uso de memoria
        for item in data:
            year = item['year']
            url = item['url']

            logging.info(f"Descargando archivo para el año {year} desde {url}.")

            # Descargar el archivo .rar
            try:
                with requests.get(url, stream=True, timeout=60) as response:
                    response.raise_for_status()
                    with tempfile.NamedTemporaryFile(delete=False, suffix='.rar') as tmp_rar:
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:
                                tmp_rar.write(chunk)
                rar_file_path = tmp_rar.name
                logging.info(f"Descargado y guardado archivo temporal {rar_file_path}.")
            except requests.exceptions.RequestException as e:
                logging.error(f"Fallo al descargar {year}.rar: {e}")
                continue
            except IOError as e:
                logging.error(f"Fallo al escribir el archivo temporal .rar para {year}: {e}")
                continue

            # Descomprimir el archivo .rar usando un directorio temporal
            with tempfile.TemporaryDirectory() as extract_dir:
                try:
                    # Verificar si WINRAR_PATH existe y es ejecutable
                    if not os.path.isfile(WINRAR_PATH) or not os.access(WINRAR_PATH, os.X_OK):
                        logging.error(f"WINRAR_PATH '{WINRAR_PATH}' no existe o no es ejecutable.")
                        os.remove(rar_file_path)
                        continue

                    subprocess.run([WINRAR_PATH, 'x', '-y', rar_file_path, extract_dir],
                                   check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    logging.info(f"Archivo {rar_file_path} descomprimido exitosamente en {extract_dir}.")
                except subprocess.CalledProcessError as e:
                    error_message = e.stderr.decode().strip()[:300]
                    logging.error(f"Fallo al descomprimir {year}.rar: {error_message}")
                    os.remove(rar_file_path)
                    continue

                # Encontrar el archivo CSV dentro del directorio extraído
                csv_files = [f for f in os.listdir(extract_dir) if f.endswith(".csv") and f"{year}" in f]
                if not csv_files:
                    logging.warning(f"No se encontró archivo CSV en {year}.rar.")
                    os.remove(rar_file_path)
                    continue

                csv_file_path = os.path.join(extract_dir, csv_files[0])
                logging.info(f"Procesando archivo CSV: {csv_file_path}")

                # Verificar si el archivo ya fue procesado
                with engine.begin() as connection:
                    query = text("SELECT COUNT(*) FROM jobs_log WHERE file_name = :file_name")
                    result = connection.execute(query, {"file_name": csv_files[0]}).scalar()
                    if result > 0:
                        logging.info(f"El archivo {csv_files[0]} ya ha sido procesado. Omitiendo.")
                        # Insertar registro de archivo omitido
                        exec_date = datetime.now()
                        try:
                            insert_query = text("""
                                INSERT INTO jobs_log (job_name, file_name, exec_date, preprocessed_at)
                                VALUES (:job_name, :file_name, :exec_date, :preprocessed_at)
                            """)
                            connection.execute(insert_query, {
                                "job_name": "enrolled_job",
                                "file_name": csv_files[0],
                                "exec_date": exec_date,
                                "preprocessed_at": item["preprocessed_at"]
                            })
                            logging.info(f"Registro del archivo omitido {csv_files[0]} en jobs_log.")
                        except SQLAlchemyError as e:
                            # Registrar solo el mensaje de error sin las filas
                            error_message = str(e)[:300]
                            logging.error(f"Fallo al registrar el archivo omitido {csv_files[0]} en jobs_log: {error_message}")
                        os.remove(rar_file_path)
                        continue
                    else:
                        logging.info(f"Procesando nuevo archivo: {csv_files[0]}")

                # Procesar el archivo CSV en chunks
                try:
                    # Leer y procesar el CSV en chunks
                    chunksize = 2000  # Puedes ajustar este valor según tus necesidades
                    for chunk in pd.read_csv(csv_file_path, sep=';', encoding='utf-8', chunksize=chunksize, dtype=str):
                        # Añadir columnas adicionales
                        chunk['year'] = year
                        chunk['preprocessed_at'] = item["preprocessed_at"]
                        chunk['processed_at'] = datetime.now()

                        # Renombrar columnas
                        chunk.rename(columns=matriculas_rename, inplace=True)

                        # Seleccionar las columnas necesarias que existen en la tabla
                        desired_columns = [
                            'periodo', 'id_matricula', 'codigo_unico', 'mrun', 'gen_alu', 'fec_nac_alumno', 
                            'rango_edad', 'anio_ing_carr_ori', 'sem_ing_carr_ori', 'anio_ing_carr_act',
                            'sem_ing_carr_act', 'tipo_instituto', 'tipo_inst_2', 'tipo_inst_3', 'cod_institucion', 
                            'institución', 'cod_sede', 'nombre_sede', 'cod_carrera', 'carrera', 'modalidad', 'jornada', 'version',
                            'tipo_plan_carr', 'dur_egreso_carrera', 'dur_titulacion', 'dur_carrera', 'region_sede', 'provincia_sede', 
                            'comuna_sede', 'grado_academico', 'nivel_carrera_det', 'nivel_carrera', 'requisito_ingreso', 'vigencia_carrera', 
                            'formato_valores', 'valor_matricula', 'valor_mensualidad', 'codigo_demre', 'area_conocimiento', 'area_carrera', 
                            'subarea_carrera', 'area_carrera_generica', 'area_profesion', 'subarea_carrera_2', 'acreditación_carrera', 
                            'acreditación_institucion', 'acre_inst_desde_hasta', 'año_acreditacion', 'costo_p_titulacion', 'costo_diploma',
                            'forma_ingreso', 'year', 'preprocessed_at', 'processed_at'
                        ]

                        # Filtrar las columnas que realmente existen en la tabla
                        existing_columns = [col for col in desired_columns if col in table_columns]
                        missing_columns = set(desired_columns) - set(existing_columns)
                        if missing_columns:
                            logging.warning(f"Faltan columnas en la tabla '{target_table}': {', '.join(missing_columns)}")
                        
                        df2 = chunk[existing_columns]

                        # Insertar el chunk en la base de datos
                        try:
                            df2.to_sql(target_table, con=engine, if_exists='append', index=False, method='multi', chunksize=500)
                            logging.info(f"Chunk de tamaño {len(df2)} insertado exitosamente en la base de datos.")
                        except SQLAlchemyError as e:
                            # Registrar solo el mensaje de error sin las filas
                            error_message = str(e)[:300]
                            logging.error(f"Fallo al insertar chunk en la base de datos: {error_message}")
                except Exception as e:
                    error_message = str(e)[:300]
                    logging.error(f"Fallo al procesar el archivo CSV {csv_files[0]}: {error_message}")
                    os.remove(rar_file_path)
                    continue

                # Registrar el archivo procesado en jobs_log
                with engine.begin() as connection:
                    exec_date = datetime.now()
                    try:
                        insert_query = text("""
                            INSERT INTO jobs_log (job_name, file_name, exec_date, preprocessed_at)
                            VALUES (:job_name, :file_name, :exec_date, :preprocessed_at)
                        """)
                        connection.execute(insert_query, {
                            "job_name": "enrolled_job",
                            "file_name": csv_files[0],
                            "exec_date": exec_date,
                            "preprocessed_at": item["preprocessed_at"]
                        })
                        logging.info(f"Registro del archivo procesado {csv_files[0]} en jobs_log.")
                    except SQLAlchemyError as e:
                        # Registrar solo el mensaje de error sin las filas
                        error_message = str(e)[:300]
                        logging.error(f"Fallo al registrar el archivo procesado {csv_files[0]} en jobs_log: {error_message}")

                # Eliminar el archivo .rar descargado
                os.remove(rar_file_path)
                logging.info(f"Archivo temporal {rar_file_path} eliminado.")

        logging.info("Todos los archivos fueron procesados exitosamente.")

    except Exception as e:
        # Registrar solo los primeros 300 caracteres del error crítico
        error_message = str(e)[:300]
        logging.critical(f"Error crítico en el script: {error_message}", exc_info=False)

    logging.info("Script main_optimized.py finalizó su ejecución.")

if __name__ == "__main__":
    main()
