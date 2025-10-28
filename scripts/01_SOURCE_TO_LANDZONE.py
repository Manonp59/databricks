import os
import zipfile
from azure.storage.blob import BlobServiceClient

def download_and_extract_txt_files(links, base_download_dir="/dbfs/tmp", base_extract_dir="/dbfs/tmp/extracted_files"):
    """
    Downloads zip files from a list [(year, url)].
    For each file, extracts only the .txt files into a year-specific folder.
    Args:
        links (list): List of tuples (year, url).
        base_download_dir (str): Directory for zip downloads.
        base_extract_dir (str): Directory for extracted txt files.
    """
    # Ensure download directory exists
    os.makedirs(base_download_dir, exist_ok=True)
    for year, url in links:
        zip_path = f"{base_download_dir}/dis-{year}-dept.zip"
        extract_path = f"{base_extract_dir}/{year}"
        os.makedirs(extract_path, exist_ok=True)

        print(f"Downloading {url} to {zip_path}")
        # Download the zip file (can be replaced by requests if preferred)
        os.system(f"wget {url} -O {zip_path}")

        print(f"Extracting {zip_path} to {extract_path}")
        # Open and extract only .txt files from the zip archive
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            for file_info in zip_ref.infolist():
                if file_info.filename.endswith('.txt'):
                    zip_ref.extract(file_info, extract_path)
                    print(f"Extracted file: {file_info.filename}")

def determine_blob_folder(filename):
    """
    Returns the blob folder based on the file name.
    Classifies files into PLV, COM, RESULT, or AUTRES based on their name.
    """
    name = filename.upper()
    if "PLV" in name:
        return "PLV"
    elif "COM" in name:
        return "COM"
    elif "RESULT" in name:
        return "RESULT"
    else:
        return "AUTRES"

def upload_file_to_blob(blob_client, file_path):
    """
    Uploads a local file to an Azure blob.
    Overwrites the blob if it already exists.
    """
    with open(file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)

def upload_folder_to_blob(local_base_path, blob_service_client, container_name):
    """
    For each year, traverses the extracted files,
    uploads each file to the appropriate Azure folder,
    skipping duplicates.
    """
    for year in range(2016, 2026):
        year_path = os.path.join(local_base_path, str(year))
        if os.path.exists(year_path):
            # Recursively traverse files for each year
            for root, dirs, files in os.walk(year_path):
                for filename in files:
                    file_path = os.path.join(root, filename)
                    folder_dest = determine_blob_folder(filename)
                    # Set the final blob name: folder/type/filename
                    blob_name = f"{folder_dest}/{filename}"

                    # Prepare Azure blob client
                    container_client = blob_service_client.get_container_client(container_name)
                    blob_client = container_client.get_blob_client(blob=blob_name)

                    # --- Skip duplicates: check existence before uploading ---
                    if blob_client.exists():
                        print(f"File {filename} already exists in {folder_dest}/, upload skipped.")
                        continue

                    # Upload the file to the proper blob folder in the container
                    upload_file_to_blob(blob_client, file_path)
                    print(f"File {filename} uploaded to {folder_dest}/ on blob storage")

# ---------- Parameters and execution ----------

# List of [(year, zip url)] to process
links = [
    ("2016","https://www.data.gouv.fr/api/1/datasets/r/0c83108b-f87b-470d-8980-6207ac93f4eb"),
    ("2017","https://www.data.gouv.fr/api/1/datasets/r/5785427b-3167-49fa-a581-aef835f0fb04"),
    ("2018","https://www.data.gouv.fr/api/1/datasets/r/e7514726-19ec-47dc-bcc3-a59c9bfa5f7f"), 
    ("2019", "https://www.data.gouv.fr/api/1/datasets/r/a6f74cfd-b4f7-44fb-8772-7884775b35e1"),
    ("2020", "https://www.data.gouv.fr/api/1/datasets/r/1913d0d6-d650-409d-a19e-b7c7f09e09a0"),
    ("2021", "https://www.data.gouv.fr/api/1/datasets/r/3c5ebbd9-f6b5-4837-a194-12bfeda7f38e"),
    ("2022", "https://www.data.gouv.fr/api/1/datasets/r/77d3151a-739e-4aab-8c34-7a15d7fea55d"),
    ("2023", "https://www.data.gouv.fr/api/1/datasets/r/96452cf0-329a-4908-8adb-8f061adcca4c"),
    ("2024", "https://www.data.gouv.fr/api/1/datasets/r/c0350599-a041-4724-9942-ad4c2ba9a7b3"),
    ("2025", "https://www.data.gouv.fr/api/1/datasets/r/6994a9f1-3f4b-4e15-a4dc-0e358a6aac13")
]

# Download and extract txt files from zip archives
download_and_extract_txt_files(links)

# Azure Data Lake Storage parameters (container and authentication)
scope_name = "my-scope"
key_name = "adls-key"
account_url = "https://datalakequaliteeau.blob.core.windows.net/"
container_name = "source"
local_base_path = "/dbfs/tmp/extracted_files"

# Retrieve ADLS key from Databricks secrets
credential = dbutils.secrets.get(scope=scope_name, key=key_name)
# Create Azure Blob Service client
blob_service_client = BlobServiceClient(account_url=account_url, credential=credential)

# Upload extracted files to Data Lake, organized by type/folder
upload_folder_to_blob(local_base_path, blob_service_client, container_name)