# scripts/load_data.py
import psycopg2
from config.settings import DATABASE

def connect_db():
    try:
        conn = psycopg2.connect(
            dbname=DATABASE['dbname'],
            user=DATABASE['user'],
            password=DATABASE['password'],
            host=DATABASE['host'],
            port=DATABASE['port']
        )
        print("Conexión exitosa a la base de datos")
        return conn
    except psycopg2.DatabaseError as e:
        print(f"Error al conectar a la base de datos: {e}")
        return None

def load_data():
    conn = connect_db()
    if conn:
        cursor = conn.cursor()
        # Aquí va la lógica para insertar datos en la base de datos
        cursor.close()
        conn.close()
    else:
        print("No se pudo conectar a la base de datos.")

if __name__ == '__main__':
    load_data()
