import os
import re
import logging
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, Integer, String, Date, ForeignKey, inspect
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import IntegrityError
from playwright.sync_api import sync_playwright

# Definir la función para configurar logging
def setup_logging():
    """
    Configura el sistema de logging.
    """
    log_directory = "/home/ubuntu/Vocational_Insight_Jobs/logs"
    log_filename = "laborum_areas.log"
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

# Cargar variables de entorno
load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_NAME = os.getenv("DB_NAME")

# Configurar SQLAlchemy
DATABASE_URI = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URI)
Session = sessionmaker(bind=engine)
session = Session()
Base = declarative_base()

# Definir las clases ORM
class LaborumArea(Base):
    __tablename__ = 'laborum_areas'
    id = Column(Integer, primary_key=True, autoincrement=True)
    nombre_area = Column(String(255), unique=True, nullable=False)

class LaborumAreaLink(Base):
    __tablename__ = 'laborum_areas_links_2'
    id = Column(Integer, primary_key=True, autoincrement=True)
    area_id = Column(Integer, ForeignKey('laborum_areas.id'), nullable=False)
    salario_promedio = Column(Integer, nullable=False)
    salarios_basados = Column(Integer, nullable=False)
    link_area = Column(String(255), nullable=False)
    executed_at = Column(String(255), nullable=False)
    month = Column(Date, nullable=False)

# Crear las tablas si no existen
def crear_tablas():
    inspector = inspect(engine)
    if not inspector.has_table('laborum_areas'):
        LaborumArea.__table__.create(engine)
        logging.info("Tabla 'laborum_areas' creada exitosamente.")
    else:
        logging.info("Tabla 'laborum_areas' ya existe.")
    
    if not inspector.has_table('laborum_areas_links_2'):
        LaborumAreaLink.__table__.create(engine)
        logging.info("Tabla 'laborum_areas_links_2' creada exitosamente.")
    else:
        logging.info("Tabla 'laborum_areas_links_2' ya existe.")

# Función para reemplazar caracteres
def reemplazar(texto):
    """
    Reemplaza vocales con acentos por sus equivalentes sin acentos,
    elimina comas y reemplaza espacios por guiones.
    """
    acentos = {
        'á': 'a', 'é': 'e', 'í': 'i', 'ó': 'o', 'ú': 'u',
        'Á': 'A', 'É': 'E', 'Í': 'I', 'Ó': 'O', 'Ú': 'U'
    }
    for acentuada, simple in acentos.items():
        texto = re.sub(acentuada, simple, texto)
    texto = re.sub(r',', '', texto)
    texto = re.sub(r'\s+', '-', texto)
    return texto.lower()

def scrape_data(url):
    """
    Scrapea los datos de salarios desde la página especificada.
    """
    logging.info(f"Iniciando scraping de {url}")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) " \
                       "AppleWebKit/537.36 (KHTML, like Gecko) " \
                       "Chrome/114.0.5735.110 Safari/537.36",
            viewport={"width": 1920, "height": 1080}
        )
        page = context.new_page()
        try:
            page.goto(url, timeout=60000)
            page.wait_for_load_state('networkidle', timeout=60000)
        except Exception as e:
            logging.error(f"Error al cargar la página {url}: {e}")
            browser.close()
            return []
        
        # Opcional: Ocultar el header si interfiere con la visualización
        try:
            # Utilizar XPath para seleccionar el header
            header = page.query_selector("xpath=//*[@id='root']/div/header")
            if header:
                header.evaluate("el => el.style.display = 'none'")
                logging.info("Header ocultado exitosamente.")
            else:
                logging.warning("Header no encontrado para ocultar.")
        except Exception as e:
            logging.warning(f"No se pudo ocultar el header: {e}")
        
        # Seleccionar todas las cards usando XPath
        # Basado en el XPath proporcionado para las tarjetas
        try:
            cards = page.query_selector_all("xpath=//*[@id='root']/div/div[3]/div/div/div/div")
        except Exception as e:
            logging.error(f"Error al seleccionar las tarjetas: {e}")
            browser.close()
            return []
        
        if not cards:
            logging.error("No se encontraron cards con el selector proporcionado.")
            browser.close()
            return []
        
        logging.info(f"Encontradas {len(cards)} cards. Extrayendo datos...")
        data = []
        for idx, card in enumerate(cards, start=1):
            try:
                # Extraer el nombre del área
                nombre_area_element = card.query_selector("xpath=./div/div[1]")
                nombre_area = nombre_area_element.inner_text().strip() if nombre_area_element else None
                
                # Extraer la media salarial
                media_salarial_element = card.query_selector("xpath=./div/div[2]/div/div[2]")
                media_salarial = media_salarial_element.inner_text().strip() if media_salarial_element else None
                
                # Extraer la cantidad de salarios pretendidos
                salarios_basados_element = card.query_selector("xpath=./div/div[2]/div/div[3]")
                salarios_basados_text = salarios_basados_element.inner_text().strip() if salarios_basados_element else None
                salarios_basados = re.findall(r'\d+', salarios_basados_text)[0] if salarios_basados_text else None
                
                # Limpiar el salario promedio
                salario_promedio = re.sub(r'[^\d]', '', media_salarial) if media_salarial else None
                
                if nombre_area and salario_promedio and salarios_basados:
                    data.append({
                        "nombre_area": nombre_area,
                        "salario_promedio": int(salario_promedio),
                        "salarios_basados": int(salarios_basados)
                    })
                    logging.info(f"Datos extraídos: {data[-1]}")
                else:
                    logging.warning(f"Datos incompletos en la card {idx}: {nombre_area}, {media_salarial}, {salarios_basados}")
            except Exception as e:
                logging.error(f"Error al extraer datos de la card {idx}: {e}")
        
        browser.close()
        return data

def guardar_en_bd(data, engine, session):
    """
    Guarda los datos extraídos en la base de datos MariaDB.
    """
    logging.info("Guardando datos en la base de datos")
    
    for entry in data:
        nombre_area = entry['nombre_area']
        
        # Verificar si el área ya existe
        area = session.query(LaborumArea).filter_by(nombre_area=nombre_area).first()
        if not area:
            # Insertar nueva área
            nueva_area = LaborumArea(nombre_area=nombre_area)
            session.add(nueva_area)
            try:
                session.commit()
                logging.info(f"Área '{nombre_area}' insertada exitosamente.")
                area = nueva_area  # Asignar el objeto recién insertado a 'area'
            except IntegrityError:
                session.rollback()
                area = session.query(LaborumArea).filter_by(nombre_area=nombre_area).first()
                if not area:
                    logging.error(f"Error al insertar el área '{nombre_area}'.")
                    continue
        else:
            logging.info(f"Área '{nombre_area}' ya existe.")
        
        # Ahora, insertar en laborum_areas_links_2
        link_area = f"https://www.laborum.cl/salarios/{reemplazar(nombre_area)}"
        month_current = datetime.now().date()
        executed_at_current = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        nuevo_link = LaborumAreaLink(
            area_id=area.id,  # Ahora 'area' está correctamente asignado
            salario_promedio=entry['salario_promedio'],
            salarios_basados=entry['salarios_basados'],
            link_area=link_area,
            executed_at=executed_at_current,
            month=month_current
        )
        session.add(nuevo_link)
    
    try:
        session.commit()
        logging.info("Todos los datos fueron insertados exitosamente en 'laborum_areas_links_2'.")
    except Exception as e:
        session.rollback()
        logging.error(f"Error al insertar datos en 'laborum_areas_links_2': {e}")

def main():
    # Crear tablas si no existen
    crear_tablas()
    
    url = "https://www.laborum.cl/salarios"
    data = scrape_data(url)
    
    if not data:
        logging.error("No se extrajeron datos. Terminando el script.")
        return
    
    # Guardar los datos en la base de datos
    guardar_en_bd(data, engine, session)

if __name__ == "__main__":
    main()


# import os
# import re
# import logging
# import pandas as pd
# from datetime import datetime
# from dotenv import load_dotenv
# from sqlalchemy import create_engine, Column, Integer, String, Date, ForeignKey, inspect
# from sqlalchemy.orm import sessionmaker, declarative_base
# from sqlalchemy.exc import IntegrityError
# from playwright.sync_api import sync_playwright

# # Definir la función para configurar logging
# def setup_logging():
#     """
#     Configura el sistema de logging.
#     """
#     log_directory = "/home/ubuntu/Vocational_Insight_Jobs/logs"
#     log_filename = "laborum_areas.log"
#     log_path = os.path.join(log_directory, log_filename)

#     # Crear directorio de logs si no existe
#     os.makedirs(log_directory, exist_ok=True)

#     # Configuración básica de logging
#     logging.basicConfig(
#         level=logging.INFO,
#         format="%(asctime)s [%(levelname)s] %(message)s",
#         handlers=[
#             logging.FileHandler(log_path),
#             logging.StreamHandler()  # Opcional: también loguear en consola
#         ]
#     )

# # Configurar logging
# setup_logging()

# # Cargar variables de entorno
# load_dotenv()

# DB_USER = os.getenv("DB_USER")
# DB_PASS = os.getenv("DB_PASS")
# DB_HOST = os.getenv("DB_HOST")
# DB_PORT = os.getenv("DB_PORT", "3306")
# DB_NAME = os.getenv("DB_NAME")

# # Configurar SQLAlchemy
# DATABASE_URI = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
# engine = create_engine(DATABASE_URI)
# Session = sessionmaker(bind=engine)
# session = Session()
# Base = declarative_base()

# # Definir las clases ORM
# class LaborumArea(Base):
#     __tablename__ = 'laborum_areas'
#     id = Column(Integer, primary_key=True, autoincrement=True)
#     nombre_area = Column(String(255), unique=True, nullable=False)

# class LaborumAreaLink(Base):
#     __tablename__ = 'laborum_areas_links_2'
#     id = Column(Integer, primary_key=True, autoincrement=True)
#     area_id = Column(Integer, ForeignKey('laborum_areas.id'), nullable=False)
#     salario_promedio = Column(Integer, nullable=False)
#     salarios_basados = Column(Integer, nullable=False)
#     link_area = Column(String(255), nullable=False)
#     executed_at = Column(String(255), nullable=False)
#     month = Column(Date, nullable=False)

# # Crear las tablas si no existen
# def crear_tablas():
#     inspector = inspect(engine)
#     if not inspector.has_table('laborum_areas'):
#         LaborumArea.__table__.create(engine)
#         logging.info("Tabla 'laborum_areas' creada exitosamente.")
#     else:
#         logging.info("Tabla 'laborum_areas' ya existe.")
    
#     if not inspector.has_table('laborum_areas_links_2'):
#         LaborumAreaLink.__table__.create(engine)
#         logging.info("Tabla 'laborum_areas_links_2' creada exitosamente.")
#     else:
#         logging.info("Tabla 'laborum_areas_links_2' ya existe.")

# # Función para reemplazar caracteres
# def reemplazar(texto):
#     """
#     Reemplaza vocales con acentos por sus equivalentes sin acentos,
#     elimina comas y reemplaza espacios por guiones.
#     """
#     acentos = {
#         'á': 'a', 'é': 'e', 'í': 'i', 'ó': 'o', 'ú': 'u',
#         'Á': 'A', 'É': 'E', 'Í': 'I', 'Ó': 'O', 'Ú': 'U'
#     }
#     for acentuada, simple in acentos.items():
#         texto = re.sub(acentuada, simple, texto)
#     texto = re.sub(r',', '', texto)
#     texto = re.sub(r'\s+', '-', texto)
#     return texto.lower()

# def scrape_data(url):
#     """
#     Scrapea los datos de salarios desde la página especificada.
#     """
#     logging.info(f"Iniciando scraping de {url}")
#     with sync_playwright() as p:
#         browser = p.chromium.launch(headless=True)
#         context = browser.new_context(
#             user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) " \
#                        "AppleWebKit/537.36 (KHTML, like Gecko) " \
#                        "Chrome/114.0.5735.110 Safari/537.36",
#             viewport={"width": 1920, "height": 1080}
#         )
#         page = context.new_page()
#         try:
#             page.goto(url, timeout=60000)
#             page.wait_for_load_state('networkidle', timeout=60000)
#         except Exception as e:
#             logging.error(f"Error al cargar la página {url}: {e}")
#             browser.close()
#             return []
        
#         # Opcional: Ocultar el header si interfiere con la visualización
#         try:
#             # Reemplaza 'header' con el selector correcto si es necesario
#             page.evaluate("document.querySelector('header').style.display = 'none';")
#             logging.info("Header ocultado exitosamente.")
#         except Exception as e:
#             logging.warning(f"No se pudo ocultar el header: {e}")
        
#         # Seleccionar todas las cards
#         # Basado en el HTML proporcionado, el selector para cada card es "div.sc-fWuAEb.jqTamR"
#         cards = page.query_selector_all("div.sc-fWuAEb.jqTamR")
        
#         if not cards:
#             logging.error("No se encontraron cards con el selector proporcionado.")
#             browser.close()
#             return []
        
#         logging.info(f"Encontradas {len(cards)} cards. Extrayendo datos...")
#         data = []
#         for idx, card in enumerate(cards, start=1):
#             try:
#                 # Extraer el nombre del área
#                 nombre_area_element = card.query_selector("div.sc-dcRxZo.dkXIm")
#                 nombre_area = nombre_area_element.inner_text().strip() if nombre_area_element else None
                
#                 # Extraer la media salarial
#                 media_salarial_element = card.query_selector("div.sc-bblaLu.sc-bTEpgF.sc-dOsOOS.ZDHAO")
#                 media_salarial = media_salarial_element.inner_text().strip() if media_salarial_element else None
                
#                 # Extraer la cantidad de salarios pretendidos
#                 salarios_basados_element = card.query_selector("div.sc-bblaLu.jLQKPN")
#                 salarios_basados_text = salarios_basados_element.inner_text().strip() if salarios_basados_element else None
#                 salarios_basados = re.findall(r'\d+', salarios_basados_text)[0] if salarios_basados_text else None
                
#                 # Limpiar el salario promedio
#                 salario_promedio = re.sub(r'[^\d]', '', media_salarial) if media_salarial else None
                
#                 if nombre_area and salario_promedio and salarios_basados:
#                     data.append({
#                         "nombre_area": nombre_area,
#                         "salario_promedio": int(salario_promedio),
#                         "salarios_basados": int(salarios_basados)
#                     })
#                     logging.info(f"Datos extraídos: {data[-1]}")
#                 else:
#                     logging.warning(f"Datos incompletos en la card {idx}: {nombre_area}, {media_salarial}, {salarios_basados}")
#             except Exception as e:
#                 logging.error(f"Error al extraer datos de la card {idx}: {e}")
        
#         browser.close()
#         return data

# def guardar_en_bd(data, engine, session):
#     """
#     Guarda los datos extraídos en la base de datos MariaDB.
#     """
#     logging.info("Guardando datos en la base de datos")
    
#     for entry in data:
#         nombre_area = entry['nombre_area']
        
#         # Verificar si el área ya existe
#         area = session.query(LaborumArea).filter_by(nombre_area=nombre_area).first()
#         if not area:
#             # Insertar nueva área
#             nueva_area = LaborumArea(nombre_area=nombre_area)
#             session.add(nueva_area)
#             try:
#                 session.commit()
#                 logging.info(f"Área '{nombre_area}' insertada exitosamente.")
#                 area = nueva_area  # Asignar el objeto recién insertado a 'area'
#             except IntegrityError:
#                 session.rollback()
#                 area = session.query(LaborumArea).filter_by(nombre_area=nombre_area).first()
#                 if not area:
#                     logging.error(f"Error al insertar el área '{nombre_area}'.")
#                     continue
#         else:
#             logging.info(f"Área '{nombre_area}' ya existe.")
        
#         # Ahora, insertar en laborum_areas_links_2
#         link_area = f"https://www.laborum.cl/salarios/{reemplazar(nombre_area)}"
#         month_current = datetime.now().date()
#         executed_at_current = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
#         nuevo_link = LaborumAreaLink(
#             area_id=area.id,  # Ahora 'area' está correctamente asignado
#             salario_promedio=entry['salario_promedio'],
#             salarios_basados=entry['salarios_basados'],
#             link_area=link_area,
#             executed_at=executed_at_current,
#             month=month_current
#         )
#         session.add(nuevo_link)
    
#     try:
#         session.commit()
#         logging.info("Todos los datos fueron insertados exitosamente en 'laborum_areas_links_2'.")
#     except Exception as e:
#         session.rollback()
#         logging.error(f"Error al insertar datos en 'laborum_areas_links_2': {e}")

# def main():
#     # Crear tablas si no existen
#     crear_tablas()
    
#     url = "https://www.laborum.cl/salarios"
#     data = scrape_data(url)
    
#     if not data:
#         logging.error("No se extrajeron datos. Terminando el script.")
#         return
    
#     # Guardar los datos en la base de datos
#     guardar_en_bd(data, engine, session)

# if __name__ == "__main__":
#     main()
