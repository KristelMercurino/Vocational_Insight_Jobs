# main.py

import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import rarfile
from io import BytesIO, StringIO
import pyunpack
from patoolib import extract_archive
from sqlalchemy import create_engine, text
import pandas as pd
import tempfile
import subprocess
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
    logging.info("Script main.py started.")

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
            and returns a list of dictionaries with the extracted data.
            """
            url = "https://datosabiertos.mineduc.cl/matricula-en-educacion-superior/"
            try:
                response = requests.get(url)
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
                    # Extract the year from the .rar file name
                    year = None
                    for y in range(2007, 2100):
                        if str(y) in href:
                            year = y
                            break

                    if year:
                        data.append({
                            'year': year,
                            'url': href,
                            'preprocessed_at': datetime.now()
                        })

            logging.info(f"Found {len(data)} .rar files to process.")
            return data

        # Extract data
        data = extract_data()

        if not data:
            logging.warning("No .rar files found to process.")
            return

        files = []
        for item in data[:1]:  # Process only the first item; adjust as needed
            year = item['year']
            url = item['url']

            logging.info(f"Downloading file for year {year} from {url}.")

            # Download the .rar file
            try:
                response = requests.get(url)
                response.raise_for_status()
                logging.info(f"Download of {year}.rar completed successfully.")
            except requests.exceptions.RequestException as e:
                logging.error(f"Failed to download {year}.rar: {e}")
                continue

            # Save the .rar file in the current directory
            rar_file_path = f"{year}.rar"
            try:
                with open(rar_file_path, 'wb') as f:
                    f.write(response.content)
                logging.info(f"Saved {rar_file_path} successfully.")
            except IOError as e:
                logging.error(f"Failed to save {rar_file_path}: {e}")
                continue

            # Decompress the .rar file using WinRAR
            try:
                subprocess.run([WINRAR_PATH, 'x', '-y', rar_file_path, os.getcwd()], check=True)
                logging.info(f"Extracted {rar_file_path} successfully.")
            except subprocess.CalledProcessError as e:
                logging.error(f"Failed to extract {year}.rar: {e}")
                continue

            # Find the CSV file in the current directory
            csv_file = None
            for file in os.listdir(os.getcwd()):
                if file.endswith(".csv") and f"{year}" in file:
                    csv_file = file
                    break

            if csv_file:
                # Check if the file has already been processed
                with engine.begin() as connection:  # Use engine.begin() for automatic commit
                    query = text("SELECT COUNT(*) FROM jobs_log WHERE file_name = :file_name")
                    result = connection.execute(query, {"file_name": csv_file}).scalar()
                    if result > 0:
                        logging.info(f"File {csv_file} has already been processed. Skipping.")
                        # Insert a log entry indicating the file was skipped
                        exec_date = datetime.now()
                        try:
                            insert_query = text("""
                                INSERT INTO jobs_log (job_name, file_name, exec_date, preprocessed_at)
                                VALUES (:job_name, :file_name, :exec_date, :preprocessed_at)
                            """)
                            connection.execute(insert_query, {
                                "job_name": "enrolled_job",
                                "file_name": csv_file,
                                "exec_date": exec_date,
                                "preprocessed_at": item["preprocessed_at"]
                            })
                            logging.info(f"Logged skipped file {csv_file} in jobs_log.")
                        except Exception as e:
                            logging.error(f"Failed to log skipped file {csv_file} in jobs_log: {e}")
                        continue
                    else:
                        logging.info(f"Processing new file: {csv_file}")
                        files.append({
                            "csv": csv_file,
                            "year": year,
                            "preprocessed_at": item["preprocessed_at"]
                        })
            else:
                logging.warning(f"No CSV file found in {year}.rar.")
                continue

        if not files:
            logging.warning("No new CSV files to process.")
            return

        # Process the CSV files
        chunksize = 2000  # Reduced to prevent memory issues
        data_df = []

        for file in files[:1]:  # Process only the first file; adjust as needed
            filename = file["csv"]
            year = file["year"]
            preprocessed_at = file["preprocessed_at"]

            logging.info(f"Processing CSV file: {filename}")

            try:
                with open(filename, 'r', encoding='utf-8') as f:
                    # Read the first line to get column names
                    columns = f.readline().strip().split(';')
                    # Add additional columns
                    columns.extend(['year', 'preprocessed_at', 'processed_at'])

                    # Read and process the file in chunks
                    while True:
                        rows = []
                        for _ in range(chunksize):
                            line = f.readline()
                            if not line:
                                break  # End of file

                            values = line.strip().split(';')
                            values.extend([year, preprocessed_at, datetime.now()])
                            # Convert the row to a dictionary
                            row_dict = dict(zip(columns, values))
                            rows.append(row_dict)

                        if not rows:
                            break  # No more rows to process

                        # Add the rows to the accumulated DataFrame
                        data_df.extend(rows)

                logging.info(f"CSV file {filename} processed successfully.")
            except Exception as e:
                logging.error(f"Failed to process CSV file {filename}: {e}")

        if not data_df:
            logging.warning("No data was processed from the CSV file.")
            return

        df = pd.DataFrame(data_df)
        logging.info("DataFrame created successfully.")

        logging.info("DataFrame Information:")
        buffer = StringIO()
        df.info(buf=buffer)
        info_str = buffer.getvalue()
        logging.info(info_str)

        # Rename the columns
        df.rename(columns=matriculas_rename, inplace=True)
        logging.info("DataFrame columns renamed successfully.")

        logging.info("Current DataFrame columns:")
        logging.info(df.columns)

        # Select the desired columns
        try:
            df2 = df[['periodo', 'id_matricula', 'codigo_unico', 'mrun', 'gen_alu', 'fec_nac_alumno', 
                      'rango_edad', 'anio_ing_carr_ori', 'sem_ing_carr_ori', 'anio_ing_carr_act',
                      'sem_ing_carr_act', 'tipo_instituto', 'tipo_inst_2', 'tipo_inst_3', 'cod_institucion', 
                      'institución', 'cod_sede', 'nombre_sede', 'cod_carrera', 'carrera', 'modalidad', 'jornada', 'version',
                      'tipo_plan_carr', 'dur_egreso_carrera', 'dur_titulacion', 'dur_carrera', 'region_sede', 'provincia_sede', 
                      'comuna_sede', 'grado_academico', 'nivel_carrera_det', 'nivel_carrera', 'requisito_ingreso', 'vigencia_carrera', 
                      'formato_valores', 'valor_matricula', 'valor_mensualidad', 'codigo_demre', 'area_conocimiento', 'area_carrera', 
                      'subarea_carrera', 'area_carrera_generica', 'area_profesion', 'subarea_carrera_2', 'acreditación_carrera', 
                      'acreditación_institucion', 'acre_inst_desde_hasta', 'año_acreditacion', 'costo_p_titulacion', 'costo_diploma',
                      'forma_ingreso']]
            logging.info("Selected columns for database insertion.")
        except KeyError as e:
            logging.error(f"Error selecting columns: {e}")
            return

        # Insert the data into the database in chunks
        chunksize_db = 10000
        total_chunks = (len(df2) + chunksize_db - 1) // chunksize_db  # Calculate total number of chunks

        for i, start in enumerate(range(0, len(df2), chunksize_db), start=1):
            end = start + chunksize_db
            logging.info(f"Inserting chunk {i} of {total_chunks} (indices {start} to {end-1})")
            try:
                df2.iloc[start:end].to_sql('registro_matriculas', con=engine, if_exists='append', index=False)
                logging.info(f"Chunk {i} inserted successfully.")
                # Remove or comment out the break after testing
                # break
            except Exception as e:
                logging.error(f"Failed to insert chunk {i}: {e}")

        logging.info("All chunks inserted successfully.")

        # Log the processed file in jobs_log
        with engine.begin() as connection:  # Use engine.begin() for automatic commit
            exec_date = datetime.now()
            try:
                insert_query = text("""
                    INSERT INTO jobs_log (job_name, file_name, exec_date, preprocessed_at)
                    VALUES (:job_name, :file_name, :exec_date, :preprocessed_at)
                """)
                connection.execute(insert_query, {
                    "job_name": "enrolled_job",
                    "file_name": filename,
                    "exec_date": exec_date,
                    "preprocessed_at": preprocessed_at
                })
                logging.info(f"Logged processed file {filename} in jobs_log.")
            except Exception as e:
                logging.error(f"Failed to log processed file {filename} in jobs_log: {e}")

        logging.info("Script main.py finished execution.")

    except Exception as e:
        logging.critical(f"Critical error in the script: {e}", exc_info=True)

    logging.info("Script main.py finished execution.")

if __name__ == "__main__":
    main()
