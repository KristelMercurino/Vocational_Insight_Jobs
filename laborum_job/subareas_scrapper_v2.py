import os
import re
import logging
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, Integer, String, Date, ForeignKey, inspect
from sqlalchemy.orm import sessionmaker, declarative_base, relationship
from sqlalchemy.exc import IntegrityError
from playwright.sync_api import sync_playwright
import pandas as pd

# Definir la función para configurar logging
def setup_logging():
    """
    Configura el sistema de logging.
    """
    log_directory = "/home/ubuntu/Vocational_Insight_Jobs/logs"
    log_filename = "laborum.log"
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

# Definir las clases ORM para áreas (Importadas)
class LaborumArea(Base):
    __tablename__ = 'laborum_areas'
    id = Column(Integer, primary_key=True, autoincrement=True)
    nombre_area = Column(String(255), unique=True, nullable=False)
    links = relationship("LaborumAreaLink", back_populates="area")
    subareas = relationship("LaborumSubarea", back_populates="area")

class LaborumAreaLink(Base):
    __tablename__ = 'laborum_areas_links_2'
    id = Column(Integer, primary_key=True, autoincrement=True)
    area_id = Column(Integer, ForeignKey('laborum_areas.id'), nullable=False)
    salario_promedio = Column(Integer, nullable=False)
    salarios_basados = Column(Integer, nullable=False)
    link_area = Column(String(255), nullable=False)
    executed_at = Column(String(255), nullable=False)
    month = Column(Date, nullable=False)
    
    area = relationship("LaborumArea", back_populates="links")

# Definir las clases ORM para subáreas
class LaborumSubarea(Base):
    __tablename__ = 'laborum_subareas'
    id = Column(Integer, primary_key=True, autoincrement=True)
    id_area = Column(Integer, ForeignKey('laborum_areas.id'), nullable=False)
    nombre_subarea = Column(String(100), nullable=False)
    created_at = Column(Date, default=datetime.utcnow)
    
    area = relationship("LaborumArea", back_populates="subareas")
    links = relationship("LaborumSubareaLink", back_populates="subarea")

    __table_args__ = (
        # Asegurar que no se repitan subáreas dentro de la misma área
        {'sqlite_autoincrement': True},
    )

class LaborumSubareaLink(Base):
    __tablename__ = 'laborum_subareas_links_2'
    id = Column(Integer, primary_key=True, autoincrement=True)
    id_subarea = Column(Integer, ForeignKey('laborum_subareas.id'), nullable=False)
    salario_promedio = Column(Integer, nullable=False)
    salarios_basados = Column(String(100), nullable=False)
    executed_at = Column(Date, nullable=False)
    month = Column(Date, nullable=False)
    
    subarea = relationship("LaborumSubarea", back_populates="links")

# Crear las tablas si no existen
def crear_tablas():
    inspector = inspect(engine)
    if not inspector.has_table('laborum_subareas'):
        LaborumSubarea.__table__.create(engine)
        logging.info("Tabla 'laborum_subareas' creada exitosamente.")
    else:
        logging.info("Tabla 'laborum_subareas' ya existe.")
    
    if not inspector.has_table('laborum_subareas_links_2'):
        LaborumSubareaLink.__table__.create(engine)
        logging.info("Tabla 'laborum_subareas_links_2' creada exitosamente.")
    else:
        logging.info("Tabla 'laborum_subareas_links_2' ya existe.")

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

def obtener_ultimos_links():
    """
    Obtiene los últimos enlaces para cada área usando la consulta proporcionada.
    """
    query = """
    WITH RankedAreas AS (
        SELECT 
            area_id, 
            link_area, 
            executed_at,
            ROW_NUMBER() OVER (PARTITION BY area_id ORDER BY executed_at DESC) AS rn
        FROM 
            laborum_areas_links_2
    )
    SELECT 
        area_id, 
        link_area
    FROM 
        RankedAreas
    WHERE 
        rn = 1;
    """
    try:
        df = pd.read_sql(query, engine)
        logging.info(f"Obtenidos {len(df)} enlaces recientes para las áreas.")
        return df
    except Exception as e:
        logging.error(f"Error al ejecutar la consulta SQL: {e}")
        return pd.DataFrame()

def scrape_subareas(area_id, link):
    """
    Scrapea los datos de subáreas desde la página de la área especificada.
    """
    logging.info(f"Scrapeando subáreas para area_id={area_id} desde {link}")
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
            page.goto(link, timeout=60000)
            page.wait_for_load_state('networkidle', timeout=60000)
        except Exception as e:
            logging.error(f"Error al cargar la página {link}: {e}")
            browser.close()
            return []
        
        # Opcional: Ocultar elementos que interfieren
        try:
            page.evaluate("document.querySelector('header').style.display = 'none';")
            logging.info("Header ocultado exitosamente.")
        except Exception as e:
            logging.warning(f"No se pudo ocultar el header: {e}")
        
        # Seleccionar todas las subcards
        # Basado en el HTML proporcionado, el selector para cada subcard es "div.sc-fWuAEb.jqTamR"
        subcards = page.query_selector_all("div.sc-fWuAEb.jqTamR")
        
        if not subcards:
            logging.error(f"No se encontraron subáreas en el enlace {link}.")
            browser.close()
            return []
        
        logging.info(f"Encontradas {len(subcards)} subáreas en la área_id={area_id}. Extrayendo datos...")
        subdata = []
        for idx, subcard in enumerate(subcards, start=1):
            try:
                # Extraer el nombre de la subárea
                nombre_subarea_element = subcard.query_selector("div.sc-dcRxZo.cikqSY")
                nombre_subarea = nombre_subarea_element.inner_text().strip() if nombre_subarea_element else None
                
                # Extraer la media salarial de la subárea
                media_salarial_element = subcard.query_selector("div.sc-bblaLu.sc-bTEpgF.sc-dOsOOS.ZDHAO")
                media_salarial = media_salarial_element.inner_text().strip() if media_salarial_element else None
                
                # Extraer la cantidad de salarios pretendidos para la subárea
                salarios_basados_element = subcard.query_selector("div.sc-bblaLu.jLQKPN")
                salarios_basados_text = salarios_basados_element.inner_text().strip() if salarios_basados_element else None
                salarios_basados = re.findall(r'\d+', salarios_basados_text)[0] if salarios_basados_text else None
                
                # Limpiar el salario promedio
                salario_promedio = re.sub(r'[^\d]', '', media_salarial) if media_salarial else None
                
                if nombre_subarea and salario_promedio and salarios_basados:
                    subdata.append({
                        "id_area": area_id,
                        "nombre_subarea": nombre_subarea,
                        "salario_promedio": int(salario_promedio),
                        "salarios_basados": salarios_basados
                    })
                    logging.info(f"Subdatos extraídos: {subdata[-1]}")
                else:
                    logging.warning(f"Datos incompletos en la subcard {idx} de area_id={area_id}: {nombre_subarea}, {media_salarial}, {salarios_basados}")
            except Exception as e:
                logging.error(f"Error al extraer datos de la subcard {idx} de area_id={area_id}: {e}")
        
        browser.close()
        return subdata

def guardar_subareas_en_bd(subdata, session):
    """
    Guarda los datos de subáreas extraídos en la base de datos.
    """
    logging.info("Guardando subáreas en la base de datos")
    
    for entry in subdata:
        nombre_subarea = entry['nombre_subarea']
        id_area = entry['id_area']
        
        # Verificar si la subárea ya existe dentro del área
        subarea = session.query(LaborumSubarea).filter_by(nombre_subarea=nombre_subarea, id_area=id_area).first()
        if not subarea:
            # Insertar nueva subárea
            nueva_subarea = LaborumSubarea(nombre_subarea=nombre_subarea, id_area=id_area)
            session.add(nueva_subarea)
            try:
                session.commit()
                logging.info(f"Subárea '{nombre_subarea}' insertada exitosamente en area_id={id_area}.")
                subarea = nueva_subarea  # Asignar el objeto recién insertado a 'subarea'
            except IntegrityError:
                session.rollback()
                subarea = session.query(LaborumSubarea).filter_by(nombre_subarea=nombre_subarea, id_area=id_area).first()
                if not subarea:
                    logging.error(f"Error al insertar la subárea '{nombre_subarea}' en area_id={id_area}.")
                    continue
        else:
            logging.info(f"Subárea '{nombre_subarea}' ya existe en area_id={id_area}.")
        
        # Insertar en laborum_subareas_links_2
        month_current = datetime.now().date()
        executed_at_current = datetime.now()
        
        nuevo_sublink = LaborumSubareaLink(
            id_subarea=subarea.id,
            salario_promedio=entry['salario_promedio'],
            salarios_basados=entry['salarios_basados'],
            executed_at=executed_at_current,
            month=month_current
        )
        session.add(nuevo_sublink)
    
    try:
        session.commit()
        logging.info("Todos los subdatos fueron insertados exitosamente en 'laborum_subareas_links_2'.")
    except Exception as e:
        session.rollback()
        logging.error(f"Error al insertar datos en 'laborum_subareas_links_2': {e}")

def scrape_areas_links():
    """
    Esta función obtiene los enlaces más recientes por área para scrapear subáreas.
    """
    logging.info("Obteniendo los últimos enlaces por área para scrapear subáreas.")
    ultimos_links_df = obtener_ultimos_links()
    return ultimos_links_df

def main():
    # Crear tablas si no existen
    crear_tablas()
    
    # Paso 1: Obtener los últimos enlaces por área
    ultimos_links_df = scrape_areas_links()
    
    if ultimos_links_df.empty:
        logging.error("No se obtuvieron enlaces recientes para las áreas. Terminando el script.")
        return
    
    # Paso 2: Iterar sobre cada enlace y scrapeo de subáreas
    for index, row in ultimos_links_df.iterrows():
        area_id = row['area_id']
        link_area = row['link_area']
        
        subdata = scrape_subareas(area_id, link_area)
        
        if subdata:
            guardar_subareas_en_bd(subdata, session)
        else:
            logging.warning(f"No se extrajeron subáreas para area_id={area_id} desde {link_area}.")
    
    logging.info("Proceso de scraping de subáreas completado exitosamente.")

if __name__ == "__main__":
    main()
