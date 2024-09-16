import boto3
import os
from botocore.exceptions import NoCredentialsError
from dotenv import load_dotenv

load_dotenv()

# Configurar cliente S3
s3_client = boto3.client('s3')

# Nombre del bucket
BUCKET_NAME = os.getenv("BUCKET_NAME")

# Carpeta donde se encuentran los archivos
DATA_FOLDER = '../data'

def upload_file_to_s3(file_name, bucket, object_name=None):
    """Sube un archivo a S3"""
    if object_name is None:
        object_name = os.path.basename(file_name)

    try:
        s3_client.upload_file(file_name, bucket, object_name)
        print(f"Archivo {file_name} subido correctamente a {object_name} en S3.")
        return True
    except NoCredentialsError:
        print("Error: Credenciales no disponibles")
        return False

def upload_folder_to_s3(folder, bucket):
    """Sube todos los archivos de una carpeta a S3"""
    for filename in os.listdir(folder):
        file_path = os.path.join(folder, filename)
        if os.path.isfile(file_path):
            success = upload_file_to_s3(file_path, bucket)
            if success:
                # Eliminar el archivo después de subirlo
                os.remove(file_path)
                print(f"Archivo {file_path} eliminado localmente después de subirlo.")
            else:
                print(f"Error al subir {file_path}. El archivo no se eliminará.")

def main():
    # Subir los archivos y eliminarlos después
    upload_folder_to_s3(DATA_FOLDER, BUCKET_NAME)

if __name__ == "__main__":
    main()
