import os
from dotenv import load_dotenv

# Cargar las variables desde el archivo .env
load_dotenv()

# Variables de entorno
image = "gcr.io/vocational-insight-api/laborum-scraper-subareas:latest"
region = "southamerica-east1"
memory = "1024Mi"
timeout = "900s"

# Construir los comandos Docker y gcloud
os.system(f"docker build -t {image} .")
os.system(f"docker push {image}")

env_vars = (
    f"DB_USER={os.getenv('DB_USER')},"
    f"DB_PASS={os.getenv('DB_PASS')},"
    f"DB_HOST={os.getenv('DB_HOST')},"
    f"DB_PORT={os.getenv('DB_PORT')},"
    f"DB_NAME={os.getenv('DB_NAME')},"
    f"PLAYWRIGHT_HEADLESS={os.getenv('PLAYWRIGHT_HEADLESS')}"
)

os.system(
    f"gcloud beta run jobs create laborum-subareas-job "
    f"--image {image} "
    f"--region {region} "
    f"--set-env-vars \"{env_vars}\" "
    f"--memory {memory} "
    f"--task-timeout {timeout}"
)
