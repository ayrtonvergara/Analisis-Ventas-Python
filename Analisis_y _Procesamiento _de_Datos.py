# -*- coding: utf-8 -*-
"""
PROYECTO: Análisis y Procesamiento de Datos con Python
------------------------------------------------------
INSTRUCCIONES: 
Asegúrate de tener el archivo 'ventas_eshopnow.xlsx' en la misma 
carpeta que este script antes de ejecutarlo.
------------------------------------------------------
"""

# **Lección 1 - Carga y Exploración Inicial**
import numpy as np
import pandas as pd

# 1. Generación de datos ficticios (1000 clientes)
n_clientes = 1000
np.random.seed(42)
ids_clientes = np.arange(1, n_clientes + 1)

montos_compras = np.random.uniform(100000, 1000000, size=n_clientes).round(0)

# 2. Operaciones matemáticas básicas
total_ventas = np.sum(montos_compras)
media_ventas = np.mean(montos_compras)
max_compra = np.max(montos_compras)
conteo_ventas = montos_compras.size

print(f"--- Resumen Lección 1 (NumPy) ---")
print(f"Total de transacciones: {conteo_ventas}")
print(f"Ventas totales: ${total_ventas:.0f}")
print(f"Promedio de venta (Media): ${media_ventas:.0f}")
print(f"Compra más alta: ${max_compra:.0f}")

# 3. Guardar los datos en archivos .npy
np.save('ids_clientes.npy', ids_clientes)
np.save('montos_compras.npy', montos_compras)
print("\n✅ Archivos .npy generados correctamente.")

"""NumPy es la base del análisis de datos porque es mucho más rápido y eficiente que las listas comunes de Python por tres razones:

Velocidad: Realiza operaciones en bloque (vectorización), lo que evita usar bucles for lentos.

Menos Memoria: Los datos se guardan de forma compacta y organizada, ocupando mucho menos espacio en el computador.

Cálculos Matemáticos: Está diseñado específicamente para ciencia y estadística, ofreciendo herramientas que Python por sí solo no tiene.

"""
#**Lección 2 - La librería Pandas**

# 1. Leer los datos preparados en NumPy y convertirlos en un Dataframe
ids = np.load('ids_clientes.npy')
montos = np.load('montos_compras.npy')

df_preliminar = pd.DataFrame({
    'id_cliente': ids,
    'monto_compra': montos
})

print("--- 2. Realizar una exploración inicial: ---")

# Visualizar primeras y últimas filas
print("\n[Primeras 5 filas]:")
print(df_preliminar.head())
print("\n[Últimas 5 filas]:")
print(df_preliminar.tail())

# Obtener estadísticas descriptivas
print("\n[Estadísticas Descriptivas]:")
print(df_preliminar.describe())

# Aplicar filtros condicionales

print("\n[Filtro: Clientes con compra histórica > $500,000]:")
filtro_alto_valor = df_preliminar[df_preliminar['monto_compra'] > 500000]
print(filtro_alto_valor.head())

# 3. Guardar el DataFrame preliminar en un archivo CSV
df_preliminar.to_csv('datos_preliminares.csv', index=False)
print("\n✅ DataFrame preliminar guardado como 'datos_preliminares.csv' para la Lección 3.")

"""Hallazgos: Al transformar los datos en un DataFrame, pasamos de tener simples listas de números a una tabla estructurada con nombres de columnas. Gracias a describe(), pudimos notar la distribución de las compras de forma instantánea, identificando el gasto promedio y los valores extremos.

Utilidad de Pandas: Pandas es clave porque permite realizar operaciones complejas (como filtros y estadísticas) en una sola línea de código. Su estructura de DataFrame es mucho más intuitiva para humanos que los arrays de NumPy, facilitando la limpieza y el guardado de datos en formatos comunes como CSV.

"""
#**Lección 3 - Obtención de datos desde archivos**
import requests
from io import StringIO

# 1. Cargar archivo CSV de la Lección 2 (Datos de NumPy)
df_preliminar = pd.read_csv('datos_preliminares.csv')

# Normalizamos nombres de columnas de los datos preliminares
df_preliminar.columns = df_preliminar.columns.str.lower().str.strip()

# 2. Incorporar nuevas fuentes de datos
df_ventas = pd.read_excel('ventas_eshopnow.xlsx')

# --- Normalizar nombres de columnas del Excel ---
df_ventas.columns = df_ventas.columns.str.lower().str.strip()

# --- Cálculo del Total de Venta ---
df_ventas['total_venta_clp'] = df_ventas['cantidad'] * df_ventas['precio_unitario_clp']

# 2.1 Web Scraping de Wikipedia (Datos Geográficos)
url = "https://es.wikipedia.org/wiki/Anexo:Ciudades_de_Chile"
headers = {'User-Agent': 'Mozilla/5.0'}
response = requests.get(url, headers=headers)
response.encoding = 'utf-8'
tablas = pd.read_html(StringIO(response.text))

df_web = None
for t in tablas:
    if 'Denominación' in t.columns:
        df_web = t[['Denominación', 'Región', 'Habitantes (2023)']]
        break

if df_web is not None:
    # --- Limpieza y Renombrado de la tabla Web ---
    df_web.rename(columns={
        'Denominación': 'ciudad',
        'Región': 'region'
    }, inplace=True)

    # Normalizamos columnas de la web a minúsculas
    df_web.columns = df_web.columns.str.lower().str.strip()

    # Limpieza de nombres de ciudad (quitamos "Gran ", paréntesis, etc.)
    df_web['ciudad'] = df_web['ciudad'].str.replace('Gran ','', regex=False).str.split(r' \(').str[0].str.strip()

    # --- Estandarizar ciudades y regiones para el merge ---
    df_ventas['ciudad'] = df_ventas['ciudad'].str.lower().str.strip()
    df_web['ciudad'] = df_web['ciudad'].str.lower().str.strip()
    df_ventas['region'] = df_ventas['region'].str.lower().str.strip()
    df_web['region'] = df_web['region'].str.lower().str.strip()

    print("✅ Datos de Wikipedia y Excel normalizados.")

# 3. UNIFICACIÓN Ventas + Wikipedia
df_consolidado = pd.merge(df_ventas, df_web, on=['ciudad', 'region'], how='left')

# 3.1 UNIFICACIÓN Consolidado + Datos Preliminares (NumPy)
# Unimos por 'id_cliente' para traer el 'monto_compra' generado al principio
df_final_total = pd.merge(df_consolidado, df_preliminar, on='id_cliente', how='left')

# 4. Guardar DataFrame
# Usamos encoding 'utf-8-sig' para que Excel reconozca eñes y tildes correctamente
df_final_total.to_csv('proyecto_final_consolidado.csv', index=False, encoding='utf-8-sig')
print("\n--- Columnas del Dataset ---")
print(df_final_total.columns)
print(f"\n✅ Proceso terminado. Filas totales: {len(df_final_total)}")

"""Desafíos encontrados:

Diferencias de formato: La tabla web contenía nombres de ciudades con notas aclaratorias (entre paréntesis) que impedían el cruce directo con el Excel. Se solucionó con limpieza de strings.

Tipografía: Fue necesario normalizar todo a minúsculas y quitar espacios en blanco (strip) porque un simple espacio extra hacía que el merge fallara.

Codificación: El uso de utf-8-sig fue vital para que los caracteres especiales de las ciudades chilenas se visualizaran correctamente al abrir el archivo en Excel.

"""
#**Lección 4 - Manejo de valores perdidos y outliers**

# Cargar el consolidado de la Lección 3
df_limpieza = pd.read_csv('proyecto_final_consolidado.csv')

# 1. Identificar valores nulos
print("--- 🔍 Conteo de Valores Nulos por Columna ---")
print(df_limpieza.isnull().sum())

# --- Estandarización ---

# 2. Borrar la columna 'habitantes (2023)' (Ya que no es necesaria)
df_limpieza.drop(columns=['habitantes (2023)'], inplace=True)
print("\n✅ Columna 'habitantes' eliminada.")

# 2.1 Imputamos con la mediana, redondeamos y convertimos a entero
df_limpieza['edad'] = df_limpieza['edad'].fillna(df_limpieza['edad'].median()).round(0).astype(int)

# 2.2 Imputación 'monto_compra' con el promedio REDONDEADO
promedio_redondeado = round(df_limpieza['monto_compra'].mean(), 0)
df_limpieza['monto_compra'] = df_limpieza['monto_compra'].fillna(promedio_redondeado)

# 2.3 Corrección de Regiones (Estandarización)

mapeo_correcciones = {
    'rm': 'Región Metropolitana de Santiago',
    'metropolitana': 'Región Metropolitana de Santiago',
    'region metropolitana': 'Región Metropolitana de Santiago',
    'valparaiso': 'Región de Valparaíso',
    'valparaíso': 'Región de Valparaíso',
    'bio bio': 'Región del Biobío',
    'biobío': 'Región del Biobío',
    'maule': 'Región del Maule',
    'la araucanía': 'Región de la Araucanía',
    'coquimbo': 'Región de Coquimbo',
    'antofagasta': 'Región de Antofagasta',
    'los lagos': 'Región de los Lagos',
    "o'higgins": "Región del Libertador General Bernardo O'Higgins"
}

df_limpieza['region'] = df_limpieza['region'].replace(mapeo_correcciones)

# 2.4 Eliminar SOLO las filas donde 'region' sea nula
df_limpieza.dropna(subset=['region'], inplace=True)

# --- 🔍 Verificación final ---
print("\n--- 🔍 Valores únicos CORREGIDOS ---")
print(df_limpieza['region'].unique())

# 3. Detectar outliers (IQR)
Q1 = df_limpieza['total_venta_clp'].quantile(0.25)
Q3 = df_limpieza['total_venta_clp'].quantile(0.75)
IQR = Q3 - Q1
limite_superior = Q3 + 1.5 * IQR

# Aplicamos Winsorizing: Ajustar valores extremos al límite superior
df_limpieza.loc[df_limpieza['total_venta_clp'] > limite_superior, 'total_venta_clp'] = limite_superior
print(f"✅ Outliers gestionados. Límite superior definido en: ${limite_superior:,.0f}")

"""Documentación de decisiones
Decisiones tomadas:

Limpieza de Regiones: Se estandarizaron los nombres de las regiones para evitar duplicados por errores de escritura (ej. "RM" vs "Metropolitana").

Imputación: Se usó la mediana para la edad para evitar sesgos y el promedio redondeado para los montos para mantener la consistencia monetaria.

Gestión de Outliers: Se utilizó la técnica de Winsorizing para limitar el impacto de ventas atípicas sin eliminar registros, asegurando que los promedios futuros sean representativos.
"""

df_limpieza.to_csv('proyecto_final_WRANGLED.csv', index=False, encoding='utf-8-sig')
print(f"✅ Archivo guardado. Filas finales: {df_limpieza.shape[0]}")

#**Lección 5 - Data Wrangling y Enriquecimiento de Datos**

# 1. Tomamos el DataFrame limpio de la Lección 4
df_wrangling = pd.read_csv('proyecto_final_WRANGLED.csv')

# 2. Aplicar tecnicas de Data Wrangling "detectar duplicados antes de borrarlos"
total_filas_antes = df_wrangling.shape[0]
duplicados = df_wrangling.duplicated(subset=['id_venta']).sum()
print(f"Registros duplicados encontrados en 'id_venta': {duplicados}")

# 2.1 Eliminar registros duplicados
antes = len(df_wrangling)
df_wrangling.drop_duplicates(inplace=True)
despues = len(df_wrangling)

print(f"✅ Se eliminaron {antes - despues} registros duplicados.")

# 2.2 Inspección de tipos de datos
print("--- 🔍 Tipos de datos ACTUALES antes de la transformación ---")
print(df_wrangling.info())

# Transformar tipos de datos
print("--- Optimizando tipos de datos ---")

# a. Transformar fechas
df_wrangling['fecha_venta'] = pd.to_datetime(df_wrangling['fecha_venta'], format='%d-%m-%Y')

# b. Transformar textos de 'object' a 'string' (Mejor para memoria y claridad)
columnas_texto = ['nombre_cliente', 'ciudad', 'region', 'canal_venta']
for col in columnas_texto:
    df_wrangling[col] = df_wrangling[col].astype("string")

# c. Transformar números a enteros (int64)
df_wrangling['edad'] = df_wrangling['edad'].astype(int)
df_wrangling['monto_compra'] = df_wrangling['monto_compra'].astype(int)

# Verificación
print("\n--- Tipos de datos OPTIMIZADOS ---")
print(df_wrangling.dtypes)

# 2.3 Crear nuevas columnas calculadas
# Vamos a crear una columna de "Venta de Fin de Semana"
# (Esto servirá para ver si la gente compra más los sábados y domingos)
df_wrangling['fin_de_semana'] = df_wrangling['fecha_venta'].dt.dayofweek.isin([5, 6])

# 2.4 Aplicar funciones personalizadas (Lambda)
# Clasifiquemos el volumen de compra: "Individual" (1 unidad) o "Pack" (más de 1)
df_wrangling['tipo_compra'] = df_wrangling['cantidad'].apply(lambda x: 'Pack' if x > 1 else 'Individual')

# 2.5 Discretizar columnas (Categorías)
# Clasifiquemos a los clientes por su edad en etapas de vida
bins = [17, 30, 50, 65, 120]
labels = ['Joven', 'Adulto Joven', 'Senior', 'Adulto Mayor']
df_wrangling['etapa_vida'] = pd.cut(df_wrangling['edad'], bins=bins, labels=labels)

print("Columnas de valor añadido creadas")
print(df_wrangling[['fin_de_semana', 'tipo_compra', 'etapa_vida']].head())

# Transformar textos de 'object' a 'string'
columnas_texto = ['tipo_compra','etapa_vida']
for col in columnas_texto:
    df_wrangling[col] = df_wrangling[col].astype("string")

# Verificar tipo de datos
print(df_wrangling.dtypes)

# Guardar el DataFrame optimizado
df_wrangling.to_csv('proyecto_final_OPTIMIZADO.csv', index=False, encoding='utf-8-sig')
print(f"\n💾 Archivo 'proyecto_final_OPTIMIZADO.csv' generado con {df_wrangling.shape[1]} columnas.")

#**Lección 6 - Agrupamiento y pivoteo de datos**

# Cargamos el DataFrame optimizado
df_final = pd.read_csv('proyecto_final_OPTIMIZADO.csv')

# 2.Aplicar técnicas de agrupamiento
# Resumen por Región
resumen_region = df_final.groupby('region').agg({
    'total_venta_clp': 'sum',
    'id_venta': 'count'
}).reset_index()

# 2.1 Resumen por Etapa de Vida
perfil_etapa = df_final.groupby('etapa_vida').agg({
    'total_venta_clp': 'mean',
    'edad': 'mean'
}).reset_index()

# Redondeo de edad
perfil_etapa['edad'] = perfil_etapa['edad'].round(0).astype(int)

# 3. (Pivot Table)
# Matriz de ventas: Región vs Canal de Venta
tabla_pivot = df_final.pivot_table(
    values='total_venta_clp',
    index='region',
    columns='canal_venta',
    aggfunc='sum',
    fill_value=0
).reset_index()

# 4.Combinar fuentes (Merge) ---
df_metas = pd.DataFrame({
    'region': df_final['region'].unique(),
    'meta_anual': [15000000] * len(df_final['region'].unique())
})
df_final_con_metas = pd.merge(df_final, df_metas, on='region', how='left')

# 5: Exportar
# A. CSV
df_final_con_metas.to_csv('PROYECTO_FINAL_ANALISIS.csv', index=False, encoding='utf-8-sig')

# B. Excel con pestañas
try:
    with pd.ExcelWriter('PROYECTO_FINAL_ANALISIS.xlsx') as writer:
        df_final_con_metas.to_excel(writer, sheet_name='1_Datos_Base_Completo', index=False)
        resumen_region.to_excel(writer, sheet_name='2_Resumen_Region', index=False)
        perfil_etapa.to_excel(writer, sheet_name='3_Perfil_Cliente', index=False)
        tabla_pivot.to_excel(writer, sheet_name='4_Matriz_Canales', index=False)

except Exception as e:
    print(f"\n❌ Error: {e}")

print("\n✅ Archivos generados correctamente.")

"""
**Documento Resumen**



Lección 1 (NumPy): Se crearon los cimientos del proyecto mediante la generación de datos aleatorios y cálculos estadísticos de alta velocidad, aprovechando la eficiencia de los arrays vectorizados.

Lección 2 (Pandas Inicial): Transformamos los datos crudos en DataFrames, permitiendo una exploración visual y estadística rápida de la información de clientes.

Lección 3 (Integración): Consolidamos información de tres mundos distintos: CSV (datos internos), Excel (ventas) y Web Scraping (datos geográficos de Wikipedia), logrando un conjunto de datos unificado.

Lección 4 (Limpieza): Se trataron valores nulos mediante imputación (mediana y promedio) y se suavizaron los outliers usando la técnica de Winsorizing para no sesgar el análisis.

Lección 5 (Wrangling): Enriquecimos el dataset creando nuevas variables como "Categoría de Edad" y "Segmento de Cliente" usando funciones lambda y técnicas de binning.

Lección 6 (Análisis): Finalmente, se generaron reportes mediante tablas dinámicas y agrupamientos, estructurando los datos para una toma de decisiones informada y exportándolos en formatos listos para el negocio.


"""