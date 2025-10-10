# proyecto_datos_2

# Base de datos
El nombre de la base de datos es Avocadobase, recuerden cambiar el nombre y la contraseña de la conexión acorde a el de su pgadmin. Recuerden crear la base de datos con el txt que creé.

# Esquema usado
En este caso, se creó una tabla de hechos llamada fact_ventas_aguacate, donde se almacenan los valores numéricos de las ventas (como el precio promedio y el volumen total). Esta tabla se relaciona con varias tablas de dimensiones, como dim_fecha, dim_producto, dim_region, dim_estado y dim_zona, que contienen información descriptiva sobre cada aspecto del dato.

Se eligió el modelo en copo de nieve porque facilita el análisis desde diferentes perspectivas (por región, por tipo de aguacate, por año, etc.) y mejora la eficiencia al consultar grandes volúmenes de datos, ya que evita la duplicación de información en las dimensiones.

Tablas resultantes:

- dim_fecha: contiene los datos temporales desglosados, con columnas como fecha_key, anio, mes, dia y semana.

- dim_producto: incluye los tipos de aguacate (type) y su código identificador (producto_key).

- dim_region: guarda las regiones (region) donde se registraron las ventas.

- dim_estado: agrupa las regiones por estado o zona geográfica.

- fact_ventas_aguacate: es la tabla central del modelo. Contiene las métricas numéricas del conjunto de datos original: averageprice, totalvolume, 4046, 4225, 4770, totalbags, smallbags, largebags y xlargebags, junto con las claves foráneas que la relacionan con las dimensiones de fecha, producto y región.

## El modelo en copo de nieve la dividió en dim_region y dim_estado, donde las regiones pertenecen a un estado o zona.