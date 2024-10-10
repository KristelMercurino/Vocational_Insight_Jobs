# main.py

import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import rarfile
from io import BytesIO
import pyunpack
from patoolib import extract_archive
from sqlalchemy import create_engine
import pandas as pd
import tempfile
import subprocess
from dotenv import load_dotenv
import logging

def setup_logging():
    """
    Configura el sistema de logging para registrar eventos en un archivo .log.
    El archivo de log se ubicará un nivel más arriba en el directorio actual.
    """
    # Obtener la ruta del directorio actual
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # Definir la ruta para el archivo de log un nivel más arriba
    log_dir = os.path.abspath(os.path.join(current_dir, os.pardir))
    log_file = os.path.join(log_dir, 'execution.log')

    # Configurar el sistema de logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()  # Opcional: también mostrar los logs en la consola
        ]
    )

def main():
    # Configurar logging
    setup_logging()
    logging.info("Inicio del script principal.")

    try:
        # Cargar variables de entorno desde el archivo .env
        load_dotenv()
        logging.info("Variables de entorno cargadas exitosamente.")

        # Configuración de la base de datos
        DB_USER = os.getenv("DB_USER")
        DB_PASS = os.getenv("DB_PASS")
        DB_HOST = os.getenv("DB_HOST")
        DB_PORT = os.getenv("DB_PORT")
        DB_NAME = os.getenv("DB_NAME")
        WINRAR_PATH = os.getenv("WINRAR_PATH", "C:\\Program Files\\WinRAR\\WinRAR.exe")

        if not all([DB_USER, DB_PASS, DB_HOST, DB_PORT, DB_NAME]):
            logging.error("Faltan variables de entorno necesarias para la configuración de la base de datos.")
            return

        # Definir la URI de conexión a la base de datos MariaDB
        DB_URI = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        logging.info(f"URI de conexión a la base de datos: {DB_URI}")

        # Crear el motor de conexión a la base de datos
        engine = create_engine(DB_URI)
        logging.info("Conexión a la base de datos establecida exitosamente.")

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
            Extrae los enlaces de los archivos .rar desde la página web, identifica el año
            y retorna una lista de diccionarios con los datos extraídos.
            """
            url = "https://datosabiertos.mineduc.cl/matricula-en-educacion-superior/"
            logging.info(f"Accediendo a la URL: {url}")
            try:
                response = requests.get(url)
                response.raise_for_status()
                logging.info("Solicitud HTTP exitosa.")
            except requests.RequestException as e:
                logging.error(f"Error al acceder a la página web: {e}")
                return []

            data = []

            try:
                soup = BeautifulSoup(response.content, "html.parser")
                links = soup.find_all('a', href=True)

                for link in links:
                    href = link['href']
                    if href.endswith('.rar'):
                        # Extraer el año del nombre del archivo .rar
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
                logging.info(f"Se encontraron {len(data)} archivos .rar para procesar.")
            except Exception as e:
                logging.exception(f"Error al procesar el contenido HTML: {e}")

            return data

        # Extraer los datos
        data = extract_data()

        if not data:
            logging.warning("No se encontraron archivos .rar para procesar.")
            return

        files = []
        for item in data[:1]:  # Procesar solo el primer elemento; ajustar según necesidad
            year = item['year']
            url = item['url']

            logging.info(f"Procesando archivo del año: {year}")
            logging.info(f"URL: {url}")

            try:
                # Descargar el archivo .rar
                response = requests.get(url)
                response.raise_for_status()
                logging.info(f"Descarga de {year}.rar exitosa.")
            except requests.RequestException as e:
                logging.error(f"Error al descargar {year}.rar: {e}")
                continue

            try:
                # Guardar el archivo .rar en la carpeta actual
                rar_file_path = os.path.join(current_dir, f"{year}.rar")
                with open(rar_file_path, 'wb') as f:
                    f.write(response.content)
                logging.info(f"Archivo {year}.rar guardado exitosamente en {rar_file_path}.")
            except Exception as e:
                logging.exception(f"Error al guardar el archivo {year}.rar: {e}")
                continue

            # Descomprimir el archivo .rar usando WinRAR
            try:
                subprocess.run([WINRAR_PATH, 'x', '-y', rar_file_path, current_dir], check=True)
                logging.info(f"Archivo {year}.rar descomprimido exitosamente.")
            except subprocess.CalledProcessError as e:
                logging.error(f"Error al descomprimir {year}.rar: {e}")
                continue

            # Buscar el archivo CSV en la carpeta actual
            csv_file = None
            try:
                for file in os.listdir(current_dir):
                    if file.endswith(".csv"):
                        csv_file = file
                        break

                if csv_file:
                    csv_file_path = os.path.join(current_dir, csv_file)
                    files.append({
                        "csv": csv_file_path,
                        "year": year,
                        "preprocessed_at": item["preprocessed_at"]
                    })
                    logging.info(f"Archivo CSV encontrado: {csv_file_path}")
                else:
                    logging.warning(f"No se encontró un archivo CSV en {year}.rar")
            except Exception as e:
                logging.exception(f"Error al buscar el archivo CSV en {year}.rar: {e}")

        if not files:
            logging.warning("No se encontraron archivos CSV para procesar.")
            return

        # Procesar los archivos CSV
        chunksize = 2000  # Reducido para evitar problemas de memoria
        data_df = []

        for file in files[:1]:  # Procesar solo el primer archivo; ajustar según necesidad
            filename = file["csv"]
            year = file["year"]
            preprocessed_at = file["preprocessed_at"]

            logging.info(f"Procesando archivo CSV: {filename}")

            try:
                with open(filename, 'r', encoding='utf-8') as f:
                    # Leer la primera línea para obtener los nombres de las columnas
                    columns = f.readline().strip().split(';')
                    # Añadir columnas adicionales
                    columns.extend(['year', 'preprocessed_at', 'processed_at'])

                    # Leer y procesar el archivo por chunks
                    while True:
                        rows = []
                        for _ in range(chunksize):
                            line = f.readline()
                            if not line:
                                break  # Terminar si no hay más líneas que leer

                            values = line.strip().split(';')
                            values.extend([year, preprocessed_at, datetime.now()])
                            # Convertir la fila en un diccionario
                            row_dict = dict(zip(columns, values))
                            rows.append(row_dict)

                        if not rows:
                            break  # Salir del loop si no hay más filas que procesar

                        # Añadir las filas al DataFrame acumulado
                        data_df.extend(rows)

                logging.info(f"Archivo CSV {filename} procesado exitosamente.")
            except Exception as e:
                logging.exception(f"Error al leer el archivo CSV {filename}: {e}")

        if not data_df:
            logging.warning("No se procesaron datos del archivo CSV.")
            return

        try:
            df = pd.DataFrame(data_df)
            logging.info("DataFrame creado exitosamente.")
        except Exception as e:
            logging.exception(f"Error al crear el DataFrame: {e}")
            return

        # Mostrar información del DataFrame
        try:
            logging.info("Información del DataFrame:")
            buffer = []
            df_info = df.info(buf=buffer)
            s = ''.join(buffer)
            logging.info(s)
        except Exception as e:
            logging.exception(f"Error al obtener la información del DataFrame: {e}")

        # Renombrar las columnas
        try:
            df.rename(columns=matriculas_rename, inplace=True)
            logging.info("Columnas renombradas exitosamente.")
        except Exception as e:
            logging.exception(f"Error al renombrar las columnas del DataFrame: {e}")

        # Mostrar las columnas renombradas
        try:
            logging.info("Columnas renombradas:")
            logging.info(df.columns.tolist())
        except Exception as e:
            logging.exception(f"Error al listar las columnas del DataFrame: {e}")

        # Seleccionar las columnas deseadas
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
            logging.info("Columnas seleccionadas para la inserción en la base de datos.")
        except Exception as e:
            logging.exception(f"Error al seleccionar las columnas del DataFrame: {e}")
            return

        # Insertar los datos en la base de datos por chunks
        chunksize_db = 10000
        total_chunks = (len(df2) + chunksize_db - 1) // chunksize_db  # Calcular el número total de chunks

        for i, start in enumerate(range(0, len(df2), chunksize_db), start=1):
            end = start + chunksize_db
            try:
                logging.info(f"Insertando chunk {i} de {total_chunks} (índices {start} a {end-1})")
                df2.iloc[start:end].to_sql('registro_matriculas_2', con=engine, if_exists='append', index=False)
                logging.info(f"Chunk {i} insertado con éxito.")
            except Exception as e:
                logging.exception(f"Error al insertar el chunk {i}: {e}")

        logging.info("Inserción de todos los chunks completada exitosamente.")

    except Exception as e:
        logging.exception(f"Error inesperado en el script principal: {e}")

    finally:
        logging.info("Finalización del script principal.")

if __name__ == "__main__":
    main()
