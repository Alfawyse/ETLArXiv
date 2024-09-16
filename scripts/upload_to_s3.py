import boto3
from botocore.exceptions import NoCredentialsError
from dotenv import load_dotenv
import os


load_dotenv()

# Parámetros de configuración
BUCKET_NAME = os.getenv('BUCKET_NAME') # Reemplaza con el nombre de tu bucket S3
DATA_FOLDER = '../data'  # Ruta de la carpeta donde están los archivos a subir

# Crear cliente de S3
s3_client = boto3.client('s3')

# Función para subir un archivo a S3
def upload_file_to_s3(file_name, bucket, object_name=None):
    """Sube un archivo a un bucket de S3.

    :param file_name: Ruta del archivo a subir
    :param bucket: Nombre del bucket de S3
    :param object_name: Nombre del objeto en S3. Si no se proporciona, será igual al nombre del archivo.
    :return: True si se subió correctamente, de lo contrario False
    """
    if object_name is None:
        object_name = os.path.basename(file_name)

    try:
        s3_client.upload_file(file_name, bucket, object_name)
        print(f"Archivo {file_name} subido exitosamente a {bucket}/{object_name}")
        return True
    except FileNotFoundError:
        print(f"El archivo {file_name} no fue encontrado.")
        return False
    except NoCredentialsError:
        print("Credenciales de AWS no disponibles.")
        return False

# Función para subir todos los archivos de la carpeta DATA_FOLDER a S3
def upload_folder_to_s3(data_folder, bucket):
    """Sube todos los archivos de la carpeta 'data_folder' al bucket S3 especificado."""
    for root, dirs, files in os.walk(data_folder):
        for file in files:
            file_path = os.path.join(root, file)
            # Opcionalmente puedes modificar el nombre del objeto en S3 aquí
            object_name = os.path.relpath(file_path, data_folder)
            upload_file_to_s3(file_path, bucket, object_name)

# Función principal
def main():
    upload_folder_to_s3(DATA_FOLDER, BUCKET_NAME)

if __name__ == "__main__":
    main()
