import os
import pandas as pd
import matplotlib.pyplot as plt
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# 1. Configuración y Conexión
load_dotenv()
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
engine = create_engine(f'postgresql://{db_user}:{db_password}@localhost:5432/climasnp')

# 2. Extracción de la Capa Plata
query = text("""
    SELECT fecha, temperatura_min, temperatura_media, temperatura_max 
    FROM clima_plata
    ORDER BY fecha ASC;
""")
df_clima = pd.read_sql(query, engine)

# Convertimos la fecha a tipo DateTime de Pandas y la hacemos el índice
df_clima['fecha'] = pd.to_datetime(df_clima['fecha'])
df_clima.set_index('fecha', inplace=True)

# 3. Transformación Analítica: Agrupamiento Anual
# .groupby(df_clima.index.year) agrupa todas las filas que compartan el mismo año
df_anual = df_clima.groupby(df_clima.index.year).mean()

# 4. Visualización Profesional
plt.figure(figsize=(12, 6))

# Dibujamos el área sombreada entre la máxima y la mínima (Amplitud térmica)
plt.fill_between(df_anual.index, 
                 df_anual['temperatura_min'], 
                 df_anual['temperatura_max'], 
                 color='orange', alpha=0.2, label='Rango (Mín a Máx)')

# Dibujamos la línea sólida de la temperatura media
plt.plot(df_anual.index, df_anual['temperatura_media'], 
         color='red', linewidth=2, marker='o', label='Temperatura Media')

# Detalles estéticos del gráfico
plt.title('Evolución Histórica de la Temperatura en Aquixtla (1990 - 2025)', fontsize=14)
plt.xlabel('Año', fontsize=12)
plt.ylabel('Temperatura Promedio (°C)', fontsize=12)
plt.grid(True, linestyle='--', alpha=0.7)
plt.legend()
plt.tight_layout()

plt.show()