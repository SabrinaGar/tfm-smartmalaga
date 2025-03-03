import json
import psycopg2
import requests

# 📌 URL del endpoint de Power BI (reemplázalo con el tuyo)
POWER_BI_STREAM_URL = "https://api.powerbi.com/beta/b4760713-c835-4043-b494-8efa9f5b2e1c/datasets/7c09c3ec-e604-44ac-a2a5-59e4d6bed1d3/rows?experience=fabric-developer&key=gORbvbOx0ODH6nR38FXKIF9BvQ%2FqIKYvipNZZjKHz5YH35HhUHQyLlEkiokJeA4weNf2y2HJ2s0EAOOgUSolsA%3D%3D"
DB_CONFIG = {
    "dbname": "smartmalaga",
    "user": "postgres",
    "password": "2025imf",
    "host": "localhost",
    "port": "5432"
}
def obtener_datos_recientes():
    """Obtiene los datos más recientes desde PostgreSQL"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT aparcamiento_id, plazas_libres, fecha
            FROM ocupacion_aparcamientos
            ORDER BY fecha DESC
            LIMIT 10
        """)
        datos = cursor.fetchall()

        cursor.close()
        conn.close()
        return datos
    
    except Exception as e:
        print(f"Error en la base de datos: {e}")
        return []

def enviar_a_power_bi():
    """Envía los datos más recientes a Power BI"""
    datos = obtener_datos_recientes()
    if not datos:
        print("No hay datos para enviar")
        return

    datos_power_bi = [
        {"aparcamiento_id": d[0], "plazas_libres": d[1], "fecha": d[2].isoformat()} for d in datos
    ]

    headers = {"Content-Type": "application/json"}
    response = requests.post(POWER_BI_STREAM_URL, headers=headers, data=json.dumps(datos_power_bi))

    if response.status_code == 200:
        print("Datos enviados a Power BI")
    else:
        print(f"Error al enviar datos: {response.text}")

# Ejecutar la función para enviar datos a Power BI
enviar_a_power_bi()
