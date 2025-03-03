import requests
import psycopg2
from datetime import datetime

# 📌 Configuración de la base de datos PostgreSQL
DB_CONFIG = {
    "dbname": "smartmalaga",
    "user": "postgres",
    "password": "2025imf",
    "host": "localhost",
    "port": "5432"
}

# 📌 API de Málaga
API_URL = "https://datosabiertos.malaga.eu/api/3/action/datastore_search?resource_id=0dcf7abd-26b4-42c8-af19-4992f1ee60c6"

def obtener_datos():
    """Obtiene los datos desde la API de Málaga"""
    response = requests.get(API_URL)
    if response.status_code == 200:
        datos = response.json()
        registros = datos["result"]["records"]

        # Extraer la info relevante
        parkings = [{"id": r["id"], "plazas_libres": r["libres"]} for r in registros]

        return parkings
    else:
        print(f"Error al obtener datos: {response.status_code}")
        return []

def guardar_en_postgresql(datos):
    """Guarda los datos en PostgreSQL"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        for parking in datos:
            cursor.execute("""
                INSERT INTO ocupacion_aparcamientos (aparcamiento_id, plazas_libres, fecha)
                VALUES (%s, %s, %s)
            """, (parking["id"], parking["plazas_libres"], datetime.now()))
        
        conn.commit()
        cursor.close()
        conn.close()
        print("Datos guardados en PostgreSQL")
    
    except Exception as e:
        print(f"Error en la base de datos: {e}")


