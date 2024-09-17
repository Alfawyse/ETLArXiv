from dotenv import load_dotenv
import os
import psycopg2

load_dotenv()

DATABASE = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'schema': os.getenv('SCHEMA'),
}

def connect_db():
    """
    Establishes a connection to the PostgreSQL database using credentials
    provided via environment variables.

    Returns:
        conn (psycopg2.connection): A connection object to interact with the PostgreSQL database.
    """
    conn = psycopg2.connect(
        dbname=DATABASE['dbname'],
        user=DATABASE['user'],
        password=DATABASE['password'],
        host=DATABASE['host'],
        port=DATABASE['port'],
    )
    return conn
