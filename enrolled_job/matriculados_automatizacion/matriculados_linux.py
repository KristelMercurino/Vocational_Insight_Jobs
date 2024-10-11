import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import subprocess
from sqlalchemy import create_engine, text
import pandas as pd
from dotenv import load_dotenv
import logging

def setup_logging():
    log_directory = "/home/ubuntu/Vocational_Insight_Jobs/logs"
    log_filename = "enrolled_job_logs.log"
    log_path = os.path.join(log_directory, log_filename)
    os.makedirs(log_directory, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.FileHandler(log_path), logging.StreamHandler()]
    )

def main():
    setup_logging()
    logging.info("Script main.py started.")

    load_dotenv()
    DB_USER = os.getenv("DB_USER")
    DB_PASS = os.getenv("DB_PASS")
    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT")
    DB_NAME = os.getenv("DB_NAME")
    WINRAR_PATH = os.getenv("WINRAR_PATH", "C:\\Program Files\\WinRAR\\WinRAR.exe")

    if not all([DB_USER, DB_PASS, DB_HOST, DB_PORT, DB_NAME]):
        logging.error("Missing required environment variables for database configuration.")
        return

    DB_URI = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
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
        url = "https://datosabiertos.mineduc.cl/matricula-en-educacion-superior/"
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")
        links = soup.find_all('a', href=True)
        logging.info(f"Found {len(links)} links on the page.")

        data = []
        for link in links:
            href = link['href']
            if href.endswith('.rar'):
                year = next((str(y) for y in range(2007, 2100) if str(y) in href), None)
                if year:
                    data.append({'year': year, 'url': href, 'preprocessed_at': datetime.now()})
        logging.info(f"Found {len(data)} .rar files to process.")
        return data

    data = extract_data()
    if not data:
        logging.warning("No .rar files found to process.")
        return

    for item in data[:1]:  # Process only the first item; adjust as needed
        year = item['year']
        url = item['url']
        logging.info(f"Downloading file for year {year} from {url}.")

        response = requests.get(url)
        response.raise_for_status()
        rar_file_path = f"{year}.rar"

        with open(rar_file_path, 'wb') as f:
            f.write(response.content)
        logging.info(f"Saved {rar_file_path} successfully.")

        # Extract .rar file
        if os.path.isfile(WINRAR_PATH) and os.access(WINRAR_PATH, os.X_OK):
            subprocess.run([WINRAR_PATH, 'x', '-y', rar_file_path, os.getcwd()], check=True)
            logging.info(f"Extracted {rar_file_path} successfully.")
        else:
            logging.error(f"WINRAR_PATH '{WINRAR_PATH}' does not exist or is not executable.")
            continue

        csv_file = next((file for file in os.listdir(os.getcwd()) if file.endswith(".csv") and f"{year}" in file), None)
        if not csv_file:
            logging.warning(f"No CSV file found in {year}.rar.")
            continue

        # Check if the file has already been processed
        with engine.begin() as connection:
            query = text("SELECT COUNT(*) FROM jobs_log WHERE file_name = :file_name")
            result = connection.execute(query, {"file_name": csv_file}).scalar()
            if result > 0:
                logging.info(f"File {csv_file} has already been processed. Skipping.")
                continue

        # Process the CSV file in chunks
        chunksize = 2000
        try:
            for chunk in pd.read_csv(csv_file, sep=';', chunksize=chunksize, iterator=True, encoding='utf-8'):
                chunk['year'] = year
                chunk['preprocessed_at'] = item['preprocessed_at']
                chunk['processed_at'] = datetime.now()

                # Rename columns
                chunk.rename(columns=matriculas_rename, inplace=True)

                # Insert into the database
                chunk.to_sql('registro_matriculas_1', con=engine, if_exists='append', index=False)
                logging.info(f"Inserted chunk for year {year} into the database.")
        except Exception as e:
            logging.error(f"Failed to process or insert CSV file {csv_file}: {e}")

        # Log the processed file in jobs_log
        with engine.begin() as connection:
            exec_date = datetime.now()
            insert_query = text("""INSERT INTO jobs_log (job_name, file_name, exec_date, preprocessed_at)
                                   VALUES (:job_name, :file_name, :exec_date, :preprocessed_at)""")
            connection.execute(insert_query, {
                "job_name": "enrolled_job",
                "file_name": csv_file,
                "exec_date": exec_date,
                "preprocessed_at": item["preprocessed_at"]
            })
            logging.info(f"Logged processed file {csv_file} in jobs_log.")

    logging.info("Script main.py finished execution.")

if __name__ == "__main__":
    main()
