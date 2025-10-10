import pandas as pd
from sqlalchemy import create_engine, text

# === CONFIGURACIÓN ===
CSV_PATH = "avocado.csv"
DB_STRING = "postgresql+psycopg2://postgres:postgres@localhost:5432/Avocadobase"

# === LECTURA Y LIMPIEZA ===
df = pd.read_csv(CSV_PATH)

# Normalizar nombres (solo para evitar problemas)
df.columns = [col.strip().lower() for col in df.columns]

# Parsear fecha
df['date_parsed'] = pd.to_datetime(df['date'], errors='coerce')

# Crear claves y columnas de fecha
df['date_key'] = df['date_parsed'].dt.strftime('%Y%m%d').astype(int)
df['year'] = df['date_parsed'].dt.year
df['month'] = df['date_parsed'].dt.month
df['day'] = df['date_parsed'].dt.day
df['week'] = df['date_parsed'].dt.isocalendar().week

# === CONEXIÓN BD ===
engine = create_engine(DB_STRING)

# === STAGING ===
df_to_stage = df[['date_key', 'date_parsed', 'averageprice', 'totalvolume',
                  '4046', '4225', '4770', 'totalbags', 'smallbags', 'largebags',
                  'xlargebags', 'type', 'year', 'region', 'month', 'day', 'week']]

df_to_stage.to_sql('stg_avocado', con=engine, schema='public',
                   if_exists='replace', index=False, method='multi')

# === INSERCIÓN A BODEGA (MODELO EN COPO DE NIEVE) ===
with engine.begin() as conn:

    # --- Dimensión Fecha ---
    conn.execute(text("""
        INSERT INTO bodega_avocado.dim_fecha (fecha_key, anio, mes, dia, semana)
        SELECT DISTINCT s.date_key, s.year, s.month, s.day, s.week
        FROM public.stg_avocado s
        WHERE NOT EXISTS (SELECT 1 FROM bodega_avocado.dim_fecha d WHERE d.fecha_key = s.date_key);
    """))

    # --- Dimensión Zona (agrupando regiones por zona aproximada) ---
    conn.execute(text("""
        INSERT INTO bodega_avocado.dim_zona (zona_nombre)
        SELECT DISTINCT CASE
            WHEN s.region ILIKE '%California%' THEN 'West'
            WHEN s.region ILIKE '%New York%' THEN 'Northeast'
            WHEN s.region ILIKE '%Texas%' THEN 'SouthCentral'
            ELSE 'Other'
        END
        FROM public.stg_avocado s
        WHERE NOT EXISTS (
            SELECT 1 FROM bodega_avocado.dim_zona z
            WHERE z.zona_nombre = CASE
                WHEN s.region ILIKE '%California%' THEN 'West'
                WHEN s.region ILIKE '%New York%' THEN 'Northeast'
                WHEN s.region ILIKE '%Texas%' THEN 'SouthCentral'
                ELSE 'Other'
            END
        );
    """))

    # --- Dimensión Estado ---
    conn.execute(text("""
        INSERT INTO bodega_avocado.dim_estado (estado_nombre, zona_key)
        SELECT DISTINCT s.region AS estado_nombre, z.zona_key
        FROM public.stg_avocado s
        JOIN bodega_avocado.dim_zona z ON
            z.zona_nombre = CASE
                WHEN s.region ILIKE '%California%' THEN 'West'
                WHEN s.region ILIKE '%New York%' THEN 'Northeast'
                WHEN s.region ILIKE '%Texas%' THEN 'SouthCentral'
                ELSE 'Other'
            END
        WHERE NOT EXISTS (
            SELECT 1 FROM bodega_avocado.dim_estado e WHERE e.estado_nombre = s.region
        );
    """))

    # --- Dimensión Región ---
    conn.execute(text("""
        INSERT INTO bodega_avocado.dim_region (region_nombre, estado_key)
        SELECT DISTINCT s.region, e.estado_key
        FROM public.stg_avocado s
        JOIN bodega_avocado.dim_estado e ON e.estado_nombre = s.region
        WHERE NOT EXISTS (
            SELECT 1 FROM bodega_avocado.dim_region r WHERE r.region_nombre = s.region
        );
    """))

    # --- Dimensión Producto ---
    conn.execute(text("""
        INSERT INTO bodega_avocado.dim_producto (tipo, anio)
        SELECT DISTINCT s.type, s.year
        FROM public.stg_avocado s
        WHERE NOT EXISTS (
            SELECT 1 FROM bodega_avocado.dim_producto p
            WHERE p.tipo = s.type AND p.anio = s.year
        );
    """))

    # --- Tabla de Hechos ---
    conn.execute(text("""
        INSERT INTO bodega_avocado.fact_ventas_aguacate (
            fecha_key, producto_key, region_key,
            average_price, total_volume, small_hass_4046, large_hass_4225, xlarge_hass_4770,
            total_bags, small_bags, large_bags, xlarge_bags
        )
        SELECT
            s.date_key,
            p.producto_key,
            r.region_key,
            s.averageprice, s.totalvolume, s."4046", s."4225", s."4770",
            s.totalbags, s.smallbags, s.largebags, s.xlargebags
        FROM public.stg_avocado s
        JOIN bodega_avocado.dim_producto p
            ON p.tipo = s.type AND p.anio = s.year
        JOIN bodega_avocado.dim_fecha f
            ON f.fecha_key = s.date_key
        JOIN bodega_avocado.dim_region r
            ON r.region_nombre = s.region;
    """))
