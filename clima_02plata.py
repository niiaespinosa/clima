import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

#
#Conexión a base de datos
#

print("Cargando credenciales...")
load_dotenv()
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')

# Creamos el motor de conexión a nuestra base de datos 'climaSNP'
engine = create_engine(f'postgresql://{db_user}:{db_password}@localhost:5432/climasnp')

query = text("""DROP TABLE IF EXISTS clima_plata;
                CREATE TABLE clima_plata AS
                SELECT 
                    CAST(jsonb_array_elements_text(raw_data -> 'daily' -> 'time') AS DATE) AS fecha,
                    CAST(jsonb_array_elements_text(raw_data -> 'daily' -> 'temperature_2m_mean') AS NUMERIC) AS temperatura_media,
                    CAST(jsonb_array_elements_text(raw_data -> 'daily' -> 'temperature_2m_min') AS NUMERIC) AS temperatura_min,
                    CAST(jsonb_array_elements_text(raw_data -> 'daily' -> 'temperature_2m_max') AS NUMERIC) AS temperatura_max,
                    CAST(jsonb_array_elements_text(raw_data -> 'daily' -> 'rain_sum') AS NUMERIC) AS lluvia,
                    CAST(jsonb_array_elements_text(raw_data -> 'daily' -> 'precipitation_sum') AS NUMERIC) AS precipitacion
                FROM clima_bronze;""")

# Usamos engine.begin() para que haga un "commit" automático si todo sale bien
with engine.begin() as conexion:
    conexion.execute(query)
    print('Carga exitosa')
