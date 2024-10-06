import os
import re
from datetime import datetime, timedelta
from sqlalchemy import create_engine, Column, Integer, String, Text, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError
from serpapi import search
from dotenv import load_dotenv  # Importar dotenv para cargar variables de entorno

# Cargar variables de entorno desde un archivo .env
load_dotenv()

# Definición de la base declarativa
Base = declarative_base()

# Modelo Noticias
class Noticias(Base):
    __tablename__ = 'noticias'
    
    id_noticia = Column(Integer, primary_key=True)
    titulo = Column(String(255), nullable=False)
    contenido = Column(Text)
    fecha_publicacion = Column(Date)
    link_noticia = Column(String(255))
    imagen_noticia = Column(String(255))

# Función para calcular la fecha
def calcular_fecha(date):
    posted_day = None
    try:
        posted_day = datetime.strptime(date, "%d-%m-%Y").strftime("%Y-%m-%d %H:%M:%S")
        return posted_day
    except ValueError:
        pass

    days_match = re.match(r'hace (\d+) días?', date)
    hours_match = re.match(r'hace (\d+) horas?', date)
    minutes_match = re.match(r'hace (\d+) mins?', date)
    seconds_match = re.match(r'hace (\d+) segundos?', date)
    
    weeks_match = re.match(r'hace (\d+) semanas?', date)
    weeks_match = re.match(r'hace (\d+) semana?', date) if weeks_match == None else weeks_match

    months_match = re.match(r'hace (\d+) meses?', date)
    months_match = re.match(r'hace (\d+) mes?', date) if months_match == None else months_match

    try:
        if months_match:
            meses = int(months_match.group(1))
            posted_day = datetime.now() - timedelta(days=30 * meses)
        elif weeks_match:
            weeks = int(weeks_match.group(1))
            posted_day = datetime.now() - timedelta(days= 7 * weeks)
        elif days_match:
            dias = int(days_match.group(1))
            posted_day = datetime.now() - timedelta(days=dias)
        elif hours_match:
            horas = int(hours_match.group(1))
            posted_day = datetime.now() - timedelta(hours=horas)
        elif minutes_match:
            minutos = int(minutes_match.group(1))
            posted_day = datetime.now() - timedelta(minutes=minutos)
        elif seconds_match:
            segundos = int(seconds_match.group(1))
            posted_day = datetime.now() - timedelta(seconds=segundos)
    except ValueError as e:
        print(f"Error al convertir a entero: {e}")

    return posted_day.strftime("%Y-%m-%d %H:%M:%S") if isinstance(posted_day, datetime) else None

# Configuración de la base de datos
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

# Definir la URI de conexión a la base de datos MariaDB
DB_URI = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Crear el motor de la base de datos
engine = create_engine(DB_URI)

# Crear una clase de sesión
Session = sessionmaker(bind=engine)

# Parámetros para la búsqueda
params = {
    "api_key": "da620831d593de36baafbfefca7e05e8e3d898aaa6176e58f3b0771c668b7b1f",
    "engine": "google",
    "q": "educacion superior en chile",
    "location": "Chile",
    "google_domain": "google.cl",
    "gl": "cl",
    "hl": "es",
    "tbm": "nws",
    "num": "50"
}

search_instance = search(params)
results = search_instance.as_dict()

def almacenar_noticias_en_db(results):
    noticias_agregadas = 0
    noticias_existentes = 0

    # Crear una nueva sesión
    session = Session()

    noticias_a_insertar = []  # Lista para las noticias a insertar en batch

    for result in results["news_results"]:
        result["posted_day"] = calcular_fecha(result["date"])

        titulo = result["title"]
        contenido = result.get("snippet", "") 
        link_noticia = result["link"]
        imagen_noticia = result.get("thumbnail", "") 
        fecha_publicacion = result["posted_day"]

        print(f"Procesando noticia: {titulo}, fecha: {fecha_publicacion}, link: {link_noticia}")

        # Verificar si la noticia con esa URL ya existe
        noticia_existente = session.query(Noticias).filter_by(link_noticia=link_noticia).first()

        if noticia_existente:
            noticias_existentes += 1
            print(f"La noticia con el link {link_noticia} ya existe, se omite.")
            continue

        # Crear una instancia de Noticia (solo si no existe)
        nueva_noticia = Noticias(
            titulo=titulo,
            contenido=contenido,
            fecha_publicacion=fecha_publicacion,
            link_noticia=link_noticia,
            imagen_noticia=imagen_noticia
        )

        noticias_a_insertar.append(nueva_noticia)  # Agregar la noticia a la lista de batch

    # Insertar todas las noticias en batch
    try:
        session.bulk_save_objects(noticias_a_insertar)  # Inserción en batch
        session.commit()  # Confirmar los cambios
        noticias_agregadas = len(noticias_a_insertar)
        print(f"Se agregaron {noticias_agregadas} noticias en batch.")
    except IntegrityError as e:
        session.rollback()
        print(f"Error al realizar la inserción en batch, error: {e}")

    # Cerrar la sesión
    session.close()

    print(f"Noticias agregadas: {noticias_agregadas}, noticias ya existentes: {noticias_existentes}")
    return {
        'total_agregadas': noticias_agregadas,
        'total_existentes': noticias_existentes
    }

# Ejecutar el almacenamiento
if __name__ == "__main__":
    almacenar_noticias_en_db(results)
