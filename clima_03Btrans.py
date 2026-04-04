import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# 1. Configuración y Conexión
print("Preparando la bóveda Oro para Machine Learning...")
load_dotenv()
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
engine = create_engine(f'postgresql://{db_user}:{db_password}@localhost:5432/climasnp')

# 2. Transformación SQL con Window Functions
query_oro = text("""
    DROP TABLE IF EXISTS clima_oro;
    
    CREATE TABLE clima_oro AS
    -- Paso 1: Limpiamos los datos sin borrar los días (convertimos a NULL)
    WITH fechas_completas AS (
        SELECT 
            fecha,
            CASE WHEN temperatura_media BETWEEN -10 AND 50 THEN temperatura_media ELSE NULL END AS temp_hoy,
            CASE WHEN precipitacion >= 0 THEN precipitacion ELSE NULL END AS lluvia_hoy
        FROM clima_plata
    )
    -- Paso 2: Unimos la tabla consigo misma (Self-Join) para saltar el año bisiesto
    SELECT 
        t1.fecha,
        t1.temp_hoy,
        t1.lluvia_hoy,
        
        -- Memoria a corto plazo
        LAG(t1.temp_hoy, 1) OVER (ORDER BY t1.fecha) AS temp_ayer,
        LAG(t1.temp_hoy, 2) OVER (ORDER BY t1.fecha) AS temp_antier,
        AVG(t1.temp_hoy) OVER (ORDER BY t1.fecha ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING) AS temp_promedio_7d,
        
        -- Memoria estacional exacta (¡A prueba de años bisiestos!)
        t2.temp_hoy AS temp_hace_un_anio
        
    FROM fechas_completas t1
    -- Hacemos match exacto con el año pasado, dejando NULL si ese día no existe
    LEFT JOIN fechas_completas t2 
           ON t1.fecha = t2.fecha + INTERVAL '1 year';
""")

with engine.begin() as conexion:
    conexion.execute(query_oro)

print("¡Capa Oro creada! Los datos ahora tienen memoria temporal.")