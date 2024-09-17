import boto3
import os
from botocore.exceptions import NoCredentialsError
from dotenv import load_dotenv

load_dotenv()

# Configure S3 client
s3_client = boto3.client('s3')

# Bucket name from environment variables
BUCKET_NAME = os.getenv("BUCKET_NAME")

# Folder containing files to upload
DATA_FOLDER = '../data'


def upload_file_to_s3(file_name, bucket, object_name=None):
    """
    Uploads a single file to the specified S3 bucket.

    Args:
        file_name (str): The path to the file to be uploaded.
        bucket (str): The S3 bucket name.
        object_name (str, optional): The S3 object name. If not provided, the file name is used.

    Returns:
        bool: True if the file was uploaded successfully, False otherwise.
    """
    if object_name is None:
        object_name = os.path.basename(file_name)

    try:
        s3_client.upload_file(file_name, bucket, object_name)
        print(f"File {file_name} successfully uploaded to {object_name} in S3.")
        return True
    except NoCredentialsError:
        print("Error: Credentials not available.")
        return False


def upload_folder_to_s3(folder, bucket):
    """
    Uploads all files from a specified folder to the S3 bucket, deleting them locally after successful upload.

    Args:
        folder (str): The folder containing the files to be uploaded.
        bucket (str): The S3 bucket name.
    """
    for filename in os.listdir(folder):
        file_path = os.path.join(folder, filename)
        if os.path.isfile(file_path):
            success = upload_file_to_s3(file_path, bucket)
            if success:
                os.remove(file_path)
                print(f"File {file_path} deleted locally after being uploaded.")
            else:
                print(f"Error uploading {file_path}. The file will not be deleted.")


def main():
    """
    Main function to upload files from the data folder to the S3 bucket and delete them locally after upload.
    """
    upload_folder_to_s3(DATA_FOLDER, BUCKET_NAME)


if __name__ == "__main__":
    main()

