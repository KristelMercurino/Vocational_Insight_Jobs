# -*- coding: utf-8 -*-

import os
import re
import logging
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd
import subprocess
from dotenv import load_dotenv
import shutil
from sqlalchemy import create_engine, Column, Integer, String, DateTime, ForeignKey, inspect
from sqlalchemy.orm import sessionmaker, declarative_base

# Cargar variables de entorno desde el archivo .env
load_dotenv()

# Configuración del logging
def setup_logging():
    """
    Configura el sistema de logging.
    """
    log_directory = os.getenv("LOG_DIRECTORY", "logs")
    log_filename = os.getenv("LOG_FILENAME", "enrolled_job_logs.log")
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

# Configurar logging
setup_logging()

# Leer variables de entorno
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_NAME = os.getenv("DB_NAME")
WINRAR_PATH = os.getenv("WINRAR_PATH")
DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR", os.getcwd())
EXTRACT_DIR = os.getenv("EXTRACT_DIR", os.path.join(DOWNLOAD_DIR, "extracted"))
OUTPUT_CSV = os.getenv("OUTPUT_CSV", os.path.join(DOWNLOAD_DIR, "processed_data.csv"))

# Configurar SQLAlchemy
DATABASE_URI = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URI)
Session = sessionmaker(bind=engine)
session = Session()
Base = declarative_base()

# Definir las clases ORM para carreras y titulados_carrera
class Carrera(Base):
    __tablename__ = 'carreras'
    id = Column(Integer, primary_key=True, autoincrement=True)
    nombre = Column(String(255), unique=True, nullable=False)
    tipo = Column(String(50), nullable=False)

class TituladoCarrera(Base):
    __tablename__ = 'titulados_carrera'
    id = Column(Integer, primary_key=True, autoincrement=True)
    id_carrera = Column(Integer, ForeignKey('carreras.id'), nullable=False)
    cantidad_titulados = Column(Integer, nullable=False)
    fecha_ejecucion = Column(DateTime, nullable=False)
    anno = Column(Integer, nullable=False)

# Crear las tablas si no existen
def crear_tablas():
    inspector = inspect(engine)
    if not inspector.has_table('carreras'):
        Carrera.__table__.create(engine)
        logging.info("Tabla 'carreras' creada exitosamente.")
    else:
        logging.info("Tabla 'carreras' ya existe.")
    
    if not inspector.has_table('titulados_carrera'):
        TituladoCarrera.__table__.create(engine)
        logging.info("Tabla 'titulados_carrera' creada exitosamente.")
    else:
        logging.info("Tabla 'titulados_carrera' ya existe.")

# Obtener los años ya procesados
def obtener_annos_existentes():
    """
    Obtiene un conjunto de años ya procesados desde la base de datos.
    """
    try:
        annos = session.query(TituladoCarrera.anno).distinct().all()
        annos_set = set([anno[0] for anno in annos])
        logging.info(f"Años ya procesados: {annos_set}")
        return annos_set
    except Exception as e:
        logging.error(f"Error al obtener años existentes: {e}")
        return set()

# Descargar y procesar un archivo .rar
def descargar_procesar_eliminar(href, anno, num_files_remaining):
    """
    Descarga, extrae, procesa y elimina un archivo .rar.
    """
    # Descargar el archivo .rar
    file_url = href if href.startswith("http") else f"https://datosabiertos.mineduc.cl{href}"
    file_name = os.path.basename(file_url)
    rar_path = os.path.join(DOWNLOAD_DIR, file_name)
    
    try:
        logging.info(f"Descargando el archivo: {file_name}")
        with requests.get(file_url, stream=True) as r:
            r.raise_for_status()
            with open(rar_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        logging.info(f"Archivo '{file_name}' descargado exitosamente en '{rar_path}'.")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error al descargar el archivo '{file_name}': {e}")
        return False

    # Extraer el archivo .rar
    extraction_success = extract_rar(rar_path, EXTRACT_DIR)
    if not extraction_success:
        logging.error(f"No se pudo extraer el archivo '{file_name}'. Eliminando el archivo .rar.")
        try:
            os.remove(rar_path)
            logging.info(f"Archivo '{rar_path}' eliminado tras fallar la extracción.")
        except Exception as e:
            logging.error(f"Error al eliminar el archivo '{rar_path}': {e}")
        return False

    # Encontrar los archivos CSV
    csv_paths = find_csv(EXTRACT_DIR)
    if not csv_paths:
        logging.error(f"No se encontraron archivos CSV en '{file_name}'. Eliminando el archivo .rar y continuando.")
        try:
            os.remove(rar_path)
            logging.info(f"Archivo '{rar_path}' eliminado tras no encontrar CSV.")
        except Exception as e:
            logging.error(f"Error al eliminar el archivo '{rar_path}': {e}")
        return False

    # Procesar cada archivo CSV encontrado
    for csv_path in csv_paths:
        processing_success = process_csv(csv_path, OUTPUT_CSV, anno)
        if not processing_success:
            logging.error(f"Hubo errores durante el procesamiento del archivo '{csv_path}'.")
            # Decidir si continuar con otros CSVs o no
            continue

    logging.info(f"Procesamiento completado para el archivo '{file_name}'.")

    # Eliminar el archivo .rar procesado
    try:
        os.remove(rar_path)
        logging.info(f"Archivo '{rar_path}' eliminado tras su procesamiento.")
    except Exception as e:
        logging.error(f"Error al eliminar el archivo '{rar_path}': {e}")

    # Limpiar el directorio de extracción para el siguiente archivo
    try:
        shutil.rmtree(EXTRACT_DIR)
        os.makedirs(EXTRACT_DIR, exist_ok=True)
        logging.info(f"Directorio de extracción '{EXTRACT_DIR}' limpiado para el siguiente archivo.")
    except Exception as e:
        logging.error(f"Error al limpiar el directorio de extracción '{EXTRACT_DIR}': {e}")

    return True

def extract_rar(rar_path, extract_to):
    """
    Extrae el contenido del archivo .rar usando WinRAR.
    """
    if not os.path.exists(rar_path):
        logging.error(f"El archivo .rar '{rar_path}' no existe.")
        return False

    # Crear directorio de extracción si no existe
    os.makedirs(extract_to, exist_ok=True)

    # Comando para extraer usando WinRAR
    # /y para sobrescribir sin preguntar
    # Extract to el directorio especificado
    command = [
        WINRAR_PATH,
        'x',      # Extracción
        '-o+',    # Sobrescribir archivos existentes sin preguntar
        rar_path,
        extract_to
    ]

    try:
        logging.info(f"Extrayendo '{rar_path}' a '{extract_to}'")
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if result.returncode != 0:
            logging.error(f"Error al extraer el archivo .rar: {result.stderr}")
            return False
        logging.info(f"Archivo '{rar_path}' extraído exitosamente.")
        return True
    except Exception as e:
        logging.error(f"Excepción durante la extracción del .rar: {e}")
        return False

def find_csv(extract_dir):
    """
    Busca todos los archivos .csv en el directorio de extracción.
    """
    csv_files = []
    for root, dirs, files in os.walk(extract_dir):
        for file in files:
            if file.endswith('.csv'):
                csv_path = os.path.join(root, file)
                logging.info(f"Archivo CSV encontrado: {csv_path}")
                csv_files.append(csv_path)
    if not csv_files:
        logging.warning("No se encontraron archivos .csv en el directorio de extracción.")
    return csv_files

def process_csv(csv_path, output_csv, year):
    """
    Procesa el archivo CSV según las especificaciones y guarda el resultado en output_csv.
    Agrega una nueva columna que indica si la carrera es técnica o profesional.
    Inserta los datos en la base de datos.
    """
    if not os.path.exists(csv_path):
        logging.error(f"El archivo CSV '{csv_path}' no existe.")
        return False

    # Nombre dinámico basado en el archivo descargado
    filename = os.path.basename(csv_path)
    logging.info(f"Procesando el archivo CSV: {filename}")

    # 1. Leer el archivo CSV en modo básico, evitando que pandas procese líneas problemáticas
    try:
        df = pd.read_csv(csv_path, delimiter=';', encoding='utf-8', skip_blank_lines=False, engine='python', on_bad_lines='skip')
        logging.info(f"Archivo cargado correctamente con {df.shape[0]} filas y {df.shape[1]} columnas.")
    except Exception as e:
        logging.error(f"Error cargando el archivo: {e}")
        return False

    # 2. Verificar si hay filas completamente vacías
    filas_vacias = df.isnull().all(axis=1).sum()
    logging.info(f"Total de filas completamente vacías: {filas_vacias}")

    # 3. Verificar si hay filas mal formateadas (con un número incorrecto de columnas)
    expected_columns = df.shape[1]  # Usar el número de columnas detectadas
    import csv

    mal_formateadas = []
    with open(csv_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile, delimiter=';')
        for i, row in enumerate(reader, start=1):
            if len(row) != expected_columns:
                mal_formateadas.append(i)
                logging.warning(f"Fila {i} tiene {len(row)} columnas en lugar de {expected_columns}")

    if mal_formateadas:
        logging.info(f"Total de filas mal formateadas: {len(mal_formateadas)}")

    # 4. Revisar cuántas filas están duplicadas
    filas_duplicadas = df.duplicated().sum()
    logging.info(f"Total de filas duplicadas: {filas_duplicadas}")

    # 5. Filtrar y contar todas las carreras
    logging.info("Contando titulados por todas las carreras.")

    # Verificar si la columna 'area_carrera_generica_n' existe
    if 'area_carrera_generica_n' not in df.columns:
        logging.error(f"La columna 'area_carrera_generica_n' no existe en el archivo '{filename}'.")
        return False

    # Obtener todas las carreras únicas
    carreras_unicas = df['area_carrera_generica_n'].dropna().unique()

    # Crear un diccionario para almacenar el número de titulados por carrera
    cant_titulados_por_carrera = {}

    # Contar el número de titulados por cada carrera
    for carrera in carreras_unicas:
        cant_titulados = df[df['area_carrera_generica_n'] == carrera].shape[0]
        # Determinar si la carrera es técnica o profesional
        if re.search(r'\bTécnico\b', carrera, re.IGNORECASE) or re.search(r'\bAnalista\b', carrera, re.IGNORECASE):
            tipo_carrera = 'Técnica'
        else:
            tipo_carrera = 'Profesional'
        cant_titulados_por_carrera[carrera] = {
            'Cantidad': cant_titulados,
            'Tipo': tipo_carrera
        }
        logging.info(f"Total de titulados en {carrera}: {cant_titulados} ({tipo_carrera})")

    # Insertar o actualizar las carreras en la base de datos
    for carrera, info in cant_titulados_por_carrera.items():
        try:
            # Verificar si la carrera ya existe
            carrera_existente = session.query(Carrera).filter_by(nombre=carrera).first()
            if not carrera_existente:
                # Insertar nueva carrera
                nueva_carrera = Carrera(nombre=carrera, tipo=info['Tipo'])
                session.add(nueva_carrera)
                session.commit()
                logging.info(f"Carrera '{carrera}' insertada en la base de datos.")
                id_carrera = nueva_carrera.id
            else:
                logging.info(f"Carrera '{carrera}' ya existe en la base de datos.")
                id_carrera = carrera_existente.id

            # Insertar en titulados_carrera
            titulados_entry = TituladoCarrera(
                id_carrera=id_carrera,
                cantidad_titulados=info['Cantidad'],
                fecha_ejecucion=datetime.now(),
                anno=year
            )
            session.add(titulados_entry)
        except Exception as e:
            session.rollback()
            logging.error(f"Error al insertar datos para la carrera '{carrera}': {e}")
            continue

    try:
        session.commit()
        logging.info(f"Datos de titulados insertados exitosamente en 'titulados_carrera'.")
    except Exception as e:
        session.rollback()
        logging.error(f"Error al insertar datos en 'titulados_carrera': {e}")
        return False

    # Crear un DataFrame con los resultados para el CSV final
    df_resultados = pd.DataFrame([
        {'Carrera': carrera, 'Cantidad': info['Cantidad'], 'Tipo': info['Tipo']}
        for carrera, info in cant_titulados_por_carrera.items()
    ])

    # Guardar el DataFrame en un CSV final
    try:
        df_resultados.to_csv(output_csv, index=False, encoding='utf-8')
        logging.info(f"Archivo CSV procesado guardado en '{output_csv}'.")
    except Exception as e:
        logging.error(f"Error al guardar el archivo CSV procesado: {e}")
        return False

    return True

def extract_and_download_files(annos_existentes, num_files=1):
    """
    Extrae los enlaces de archivos .rar de la página especificada y retorna una lista de enlaces
    que no han sido procesados previamente (basado en 'anno').
    """
    url = "https://datosabiertos.mineduc.cl/titulados-en-educacion-superior/"
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

    rar_links = [link['href'] for link in links if link['href'].endswith('.rar')]
    logging.info(f"Se encontraron {len(rar_links)} archivos .rar en la página.")

    # Filtrar RARs cuyos años ya han sido procesados
    rar_links_filtrados = []
    for href in rar_links:
        # Extraer el año del nombre del archivo .rar
        # Asumiendo que el año está en el nombre, por ejemplo: '20240628_Titulados_Ed_Superior_2023_WEB.rar'
        file_name = os.path.basename(href)
        year_match = re.search(r'(\d{4})', file_name)
        if year_match:
            anno = int(year_match.group(1))
            if anno not in annos_existentes:
                rar_links_filtrados.append((href, anno))
                logging.info(f"RAR '{file_name}' con año {anno} será procesado.")
            else:
                logging.info(f"RAR '{file_name}' con año {anno} ya ha sido procesado. Se omitirá.")
        else:
            logging.warning(f"No se pudo extraer el año del nombre del archivo '{file_name}'. Se omitirá.")

    # Asegurar que no se exceda el número de archivos disponibles
    available_files = len(rar_links_filtrados)
    if num_files > available_files:
        logging.warning(f"El número solicitado de archivos ({num_files}) excede los disponibles ({available_files}).")
        num_files = available_files

    rar_links_filtrados = rar_links_filtrados[:num_files]

    for href, anno in rar_links_filtrados:
        data.append({'href': href, 'anno': anno})

    return data

def main(num_files=1):
    """
    Coordina la ejecución de la descarga, extracción y procesamiento de archivos .rar.
    """
    # Crear tablas si no existen
    crear_tablas()

    # Obtener los años ya procesados
    annos_existentes = obtener_annos_existentes()

    # Paso 1: Obtener los archivos .rar a procesar
    rar_links = extract_and_download_files(annos_existentes, num_files)
    if not rar_links:
        logging.error("No hay archivos .rar disponibles para procesar. Terminando el script.")
        return

    # Procesar cada archivo .rar uno a la vez
    for rar in rar_links:
        href = rar['href']
        anno = rar['anno']
        logging.info(f"Inicio del procesamiento para el año {anno}.")

        # Descargar, extraer, procesar y eliminar el archivo .rar
        success = descargar_procesar_eliminar(href, anno, num_files)
        if not success:
            logging.error(f"Fallo en el procesamiento para el año {anno}. Continuando con el siguiente archivo.")
            continue

    logging.info("Todos los archivos han sido procesados.")

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Descargar, extraer y procesar archivos .rar de datosabiertos.mineduc.cl.")
    parser.add_argument('--num-files', type=int, default=1, help='Número de archivos .rar a descargar y procesar.')
    args = parser.parse_args()

    main(num_files=args.num_files)
