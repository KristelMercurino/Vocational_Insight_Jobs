import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import subprocess
import pandas as pd
from sqlalchemy import create_engine, text
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
        handlers=[
            logging.FileHandler(log_path),
            logging.StreamHandler()  # Optional: also log to console
        ]
    )

def main():
    setup_logging()
    logging.info("Script main.py started.")

    try:
        load_dotenv()
        logging.info("Environment variables loaded successfully.")

        # Database configuration
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
        logging.info("Database connection URI constructed.")

        engine = create_engine(DB_URI)
        logging.info("Database engine created successfully.")

        matriculas_rename = {  # Column renaming dictionary
            "cat_periodo": "periodo",
            "id": "id_matricula",
            # Add other mappings as necessary
        }

        def extract_data():
            url = "https://datosabiertos.mineduc.cl/matricula-en-educacion-superior/"
            try:
                response = requests.get(url)
                response.raise_for_status()
                soup = BeautifulSoup(response.content, "html.parser")
                links = soup.find_all('a', href=True)

                data = []
                for link in links:
                    href = link['href']
                    if href.endswith('.rar'):
                        year = next((str(y) for y in range(2007, 2100) if str(y) in href), None)
                        if year:
                            data.append({'year': year, 'url': href, 'preprocessed_at': datetime.now()})
                return data
            except requests.exceptions.RequestException as e:
                logging.error(f"HTTP request failed: {e}")
                return []

        # Extract data
        data = extract_data()
        if not data:
            logging.warning("No .rar files found to process.")
            return

        for item in data[:1]:  # Process only the first item
            year = item['year']
            url = item['url']

            logging.info(f"Downloading file for year {year} from {url}.")
            try:
                response = requests.get(url)
                response.raise_for_status()
                rar_file_path = f"{year}.rar"
                with open(rar_file_path, 'wb') as f:
                    f.write(response.content)

                subprocess.run([WINRAR_PATH, 'x', '-y', rar_file_path, os.getcwd()], check=True)
                logging.info(f"Extracted {rar_file_path} successfully.")

                # Process CSV files directly
                csv_file = next((file for file in os.listdir(os.getcwd()) if file.endswith(".csv") and f"{year}" in file), None)

                if csv_file:
                    with engine.begin() as connection:
                        # Check if the file has already been processed
                        query = text("SELECT COUNT(*) FROM jobs_log WHERE file_name = :file_name")
                        result = connection.execute(query, {"file_name": csv_file}).scalar()
                        if result > 0:
                            logging.info(f"File {csv_file} has already been processed. Skipping.")
                            continue

                        # Process the CSV file in chunks
                        chunksize = 2000
                        for chunk in pd.read_csv(csv_file, sep=';', chunksize=chunksize):
                            # Rename columns
                            chunk.rename(columns=matriculas_rename, inplace=True)
                            chunk['year'] = year
                            chunk['preprocessed_at'] = item['preprocessed_at']
                            chunk['processed_at'] = datetime.now()

                            # Insert the chunk into the database
                            chunk.to_sql('registro_matriculas_1', con=engine, if_exists='append', index=False)
                            logging.info(f"Inserted chunk of {len(chunk)} rows successfully.")

                        # Log the processed file
                        exec_date = datetime.now()
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
                        logging.info(f"Logged processed file {csv_file} in jobs_log.")
                else:
                    logging.warning(f"No CSV file found in {year}.rar.")

            except Exception as e:
                logging.error(f"Error processing {year}.rar: {e}")

        logging.info("Script main.py finished execution.")

    except Exception as e:
        logging.critical(f"Critical error in the script: {e}", exc_info=True)

if __name__ == "__main__":
    main()
