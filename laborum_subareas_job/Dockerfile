# Dockerfile

# Usar una imagen base de Python ligera
FROM python:3.11-slim

# Establecer variables de entorno para evitar mensajes interactivos durante la instalación
ENV DEBIAN_FRONTEND=noninteractive

# Instalar dependencias del sistema necesarias para Playwright
RUN apt-get update && apt-get install -y --no-install-recommends \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libatspi2.0-0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libpango-1.0-0 \
    libcairo2 \
    libasound2 \
    libnss3 \
    libxss1 \
    libxtst6 \
    fonts-liberation \
    libappindicator3-1 \
    libgtk-3-0 \
    xdg-utils \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Establecer el directorio de trabajo dentro del contenedor
WORKDIR /app

# Copiar el archivo de dependencias y luego instalarlas
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Instalar los navegadores de Playwright y sus dependencias
RUN playwright install --with-deps

# Copiar el resto del código de la aplicación al contenedor
COPY . .

# Exponer el puerto (opcional, ya que Cloud Run usa 8080 por defecto, pero no es necesario aquí)
# EXPOSE 8080

# Definir el comando por defecto para ejecutar el script
CMD ["python", "subareas_scrapper_v2.py"]

#docker build -t laborum-scraper:latest .
#docker run --env-file .env -it laborum-scraper:latest