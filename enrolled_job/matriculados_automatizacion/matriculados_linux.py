# main_optimized.py

import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import subprocess
from sqlalchemy import create_engine, text
import pandas as pd
import tempfile
from dotenv import load_dotenv
import logging

def setup_logging():
    """
    Configures the logging system.
    """
    log_directory = "/home/ubuntu/Vocational_Insight_Jobs/logs"
    log_filename = "enrolled_job_logs.log"
    log_path = os.path.join(log_directory, log_filename)

    # Create log directory if it doesn't exist
    os.makedirs(log_directory, exist_ok=True)

    # Basic logging configuration
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_path),
            logging.StreamHandler()  # Optional: also log to console
        ]
    )

def main():
    # Configure logging
    setup_logging()
    logging.info("Script main_optimized.py started.")

    try:
        # Load environment variables from .env file
        load_dotenv()
        logging.info("Environment variables loaded successfully.")

        # Database configuration
        DB_USER = os.getenv("DB_USER")
        DB_PASS = os.getenv("DB_PASS")
        DB_HOST = os.getenv("DB_HOST")
        DB_PORT = os.getenv("DB_PORT")
        DB_NAME = os.getenv("DB_NAME")
        WINRAR_PATH = os.getenv("WINRAR_PATH", "C:\\Program Files\\WinRAR\\WinRAR.exe")

        # Verify all necessary environment variables are present
        if not all([DB_USER, DB_PASS, DB_HOST, DB_PORT, DB_NAME]):
            logging.error("Missing required environment variables for database configuration.")
            return

        # Define the MariaDB connection URI
        DB_URI = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        logging.info("Database connection URI constructed.")

        # Create the database engine
        engine = create_engine(DB_URI)
        logging.info("Database engine created successfully.")

        # Dictionary for renaming columns
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
            Extracts .rar file links from the specified webpage, identifies the year,
            and returns a list of dictionaries con la información necesaria.
            """
            url = "https://datosabiertos.mineduc.cl/matricula-en-educacion-superior/"
            try:
                response = requests.get(url, timeout=30)
                logging.info(f"Accessing URL: {url}")
                response.raise_for_status()
            except requests.exceptions.RequestException as e:
                logging.error(f"HTTP request failed: {e}")
                return []

            data = []

            soup = BeautifulSoup(response.content, "html.parser")
            links = soup.find_all('a', href=True)
            logging.info(f"Found {len(links)} links on the page.")

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

            logging.info(f"Found {len(data)} .rar files to process.")
            return data

        # Extraer datos
        data = extract_data()

        if not data:
            logging.warning("No .rar files found to process.")
            return

        # Procesar los archivos .rar uno por uno para minimizar el uso de memoria
        for item in data:
            year = item['year']
            url = item['url']

            logging.info(f"Downloading file for year {year} from {url}.")

            # Descargar el archivo .rar
            try:
                with requests.get(url, stream=True, timeout=60) as response:
                    response.raise_for_status()
                    with tempfile.NamedTemporaryFile(delete=False, suffix='.rar') as tmp_rar:
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:
                                tmp_rar.write(chunk)
                rar_file_path = tmp_rar.name
                logging.info(f"Downloaded and saved temporary file {rar_file_path}.")
            except requests.exceptions.RequestException as e:
                logging.error(f"Failed to download {year}.rar: {e}")
                continue
            except IOError as e:
                logging.error(f"Failed to write temporary .rar file for {year}: {e}")
                continue

            # Descomprimir el archivo .rar usando un directorio temporal
            with tempfile.TemporaryDirectory() as extract_dir:
                try:
                    # Verificar si WINRAR_PATH existe y es ejecutable
                    if not os.path.isfile(WINRAR_PATH) or not os.access(WINRAR_PATH, os.X_OK):
                        logging.error(f"WINRAR_PATH '{WINRAR_PATH}' does not exist or is not executable.")
                        os.remove(rar_file_path)
                        continue

                    subprocess.run([WINRAR_PATH, 'x', '-y', rar_file_path, extract_dir], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    logging.info(f"Extracted {rar_file_path} successfully to {extract_dir}.")
                except subprocess.CalledProcessError as e:
                    logging.error(f"Failed to extract {year}.rar: {e.stderr.decode().strip()}")
                    os.remove(rar_file_path)
                    continue

                # Encontrar el archivo CSV dentro del directorio extraído
                csv_files = [f for f in os.listdir(extract_dir) if f.endswith(".csv") and f"{year}" in f]
                if not csv_files:
                    logging.warning(f"No CSV file found in {year}.rar.")
                    os.remove(rar_file_path)
                    continue

                csv_file_path = os.path.join(extract_dir, csv_files[0])
                logging.info(f"Processing CSV file: {csv_file_path}")

                # Verificar si el archivo ya fue procesado
                with engine.begin() as connection:
                    query = text("SELECT COUNT(*) FROM jobs_log WHERE file_name = :file_name")
                    result = connection.execute(query, {"file_name": csv_files[0]}).scalar()
                    if result > 0:
                        logging.info(f"File {csv_files[0]} has already been processed. Skipping.")
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
                            logging.info(f"Logged skipped file {csv_files[0]} in jobs_log.")
                        except Exception as e:
                            logging.error(f"Failed to log skipped file {csv_files[0]} in jobs_log: {e}")
                        os.remove(rar_file_path)
                        continue
                    else:
                        logging.info(f"Processing new file: {csv_files[0]}")

                # Procesar el archivo CSV en chunks
                try:
                    # Definir los nombres de las columnas
                    columns = None  # Pandas lo detecta automáticamente

                    # Leer y procesar el CSV en chunks
                    chunksize = 2000  # Puedes ajustar este valor según tus necesidades
                    for chunk in pd.read_csv(csv_file_path, sep=';', encoding='utf-8', chunksize=chunksize, dtype=str):
                        # Añadir columnas adicionales
                        chunk['year'] = year
                        chunk['preprocessed_at'] = item["preprocessed_at"]
                        chunk['processed_at'] = datetime.now()

                        # Renombrar columnas
                        chunk.rename(columns=matriculas_rename, inplace=True)

                        # Seleccionar las columnas necesarias
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

                        # Verificar si todas las columnas existen
                        missing_cols = set(desired_columns) - set(chunk.columns)
                        if missing_cols:
                            logging.error(f"Missing columns in the chunk: {missing_cols}")
                            continue

                        df2 = chunk[desired_columns]

                        # Insertar el chunk en la base de datos
                        try:
                            df2.to_sql('registro_matriculas_1', con=engine, if_exists='append', index=False, method='multi', chunksize=500)
                            logging.info(f"Inserted a chunk of size {len(df2)} into the database.")
                        except Exception as e:
                            logging.error(f"Failed to insert chunk into the database: {e}")

                    logging.info(f"CSV file {csv_files[0]} processed successfully.")
                except Exception as e:
                    logging.error(f"Failed to process CSV file {csv_files[0]}: {e}")
                    os.remove(rar_file_path)
                    continue

                # Log the processed file en jobs_log
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
                        logging.info(f"Logged processed file {csv_files[0]} in jobs_log.")
                    except Exception as e:
                        logging.error(f"Failed to log processed file {csv_files[0]} in jobs_log: {e}")

                # Eliminar el archivo .rar descargado
                os.remove(rar_file_path)
                logging.info(f"Removed temporary file {rar_file_path}.")

        logging.info("All files processed successfully.")

    except Exception as e:
        logging.critical(f"Critical error in the script: {e}", exc_info=True)

    logging.info("Script main_optimized.py finished execution.")

if __name__ == "__main__":
    main()
