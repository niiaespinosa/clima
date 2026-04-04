import os
import json
import requests_cache
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# ==========================================
# Conexión a base de datos
# ==========================================
print("Cargando credenciales...")
load_dotenv()
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')

# Creamos el motor de conexión a nuestra base de datos 'climasnp'
engine = create_engine(f'postgresql://{db_user}:{db_password}@localhost:5432/climasnp')


# ==========================================
# Open API (Con Caché y Reintentos)
# ==========================================
print("Configurando cliente web robusto...")

# 1. Configuramos el caché (si repites la petición, no gasta el límite de la API)
session = requests_cache.CachedSession('.cache', expire_after=-1)

# 2. Configuramos los reintentos (si el internet falla, intenta 5 veces)
retries = Retry(total=5, backoff_factor=0.2, status_forcelist=[500, 502, 503, 504])
session.mount('https://', HTTPAdapter(max_retries=retries))

url = "https://archive-api.open-meteo.com/v1/archive"
params = {
    "latitude": 19.7958,
    "longitude": -97.9355,
    "start_date": "1990-01-01",
    "end_date": "2026-01-01",
    "daily": ["temperature_2m_max", "temperature_2m_min", "temperature_2m_mean", "precipitation_sum", "rain_sum"],
    "timezone": "auto",
}

print('Descargando 26 años de datos históricos en formato JSON...')
# Hacemos la petición usando nuestra sesión súper robusta
respuesta = session.get(url, params=params)

# Ahora sí podemos usar esto, porque es una respuesta HTTP estándar
respuesta.raise_for_status()

# Convertimos directamente a texto JSON plano
datos_json_crudos = json.dumps(respuesta.json())
print("¡Descarga exitosa!")

# ==========================================
# Insertar a capa bronce (en SQL)
# ==========================================
print("Inyectando datos crudos en PostgreSQL...")

query_insercion = text("""
    INSERT INTO clima_bronze (raw_data) 
    VALUES (:json_data);
""")

with engine.begin() as conexion:
    conexion.execute(query_insercion, {"json_data": datos_json_crudos})

print("¡Proceso ELT (Extracción y Carga) finalizado con éxito!")