import pandas as pd
from sqlalchemy import create_engine

DB_STRING = "postgresql+psycopg2://postgres:postgres@localhost:5432/Avocadobase"

engine = create_engine(DB_STRING)

# Lista de tablas a revisar
tablas = [
    "bodega_avocado.dim_fecha",
    "bodega_avocado.dim_producto",
    "bodega_avocado.dim_zona",
    "bodega_avocado.dim_estado",
    "bodega_avocado.dim_region",
    "bodega_avocado.fact_ventas_aguacate"
]

print("🔍 Verificando tablas del modelo en copo de nieve:\n")

for tabla in tablas:
    try:
        count = pd.read_sql(f"SELECT COUNT(*) AS registros FROM {tabla};", engine)
        print(f" {tabla}: {int(count.iloc[0]['registros'])} registros")
    except Exception as e:
        print(f"Error accediendo a {tabla}: {e}")

print("\nEjemplo de revisión de datos:")
try:
    muestra = pd.read_sql("SELECT * FROM bodega_avocado.fact_ventas_aguacate LIMIT 5;", engine)
    print(muestra)
except Exception as e:
    print(f"No se pudo obtener muestra de fact_ventas_aguacate: {e}")
