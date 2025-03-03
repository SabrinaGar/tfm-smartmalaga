import time
from script_almacenamiento import obtener_datos, guardar_en_postgresql
from enviar_datos_powerBI import enviar_a_power_bi

def main():
    while True:
        # Ejecutar el proceso de extracción y almacenamiento
        # Obtener datos de la API
        data = obtener_datos()
        
        # Guardar datos en PostgreSQL
        if data:
            guardar_en_postgresql(data)
        
        # Enviar datos a Power BI
        enviar_a_power_bi()
        
        # Esperar 1 minutos antes de la siguiente ejecución
        time.sleep(60)

if __name__ == "__main__":
    main()