import zipfile
import os


def extract_zip(zip_path, extract_path, logger):
    # Ensure destination exists
    os.makedirs(extract_path, exist_ok=True)

    # Check if zip file exists
    if not os.path.exists(zip_path):
        logger.error(f"EXTRACT | Zip file not found: {zip_path}")
        raise FileNotFoundError(zip_path)

    logger.info(f"EXTRACT | Starting extraction: {zip_path}")

    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_path)

        logger.info(f"EXTRACT | Extraction complete to: {extract_path}")

    except zipfile.BadZipFile:
        logger.error(f"EXTRACT | Invalid zip file: {zip_path}")
        raise

    except Exception as e:
        logger.error(f"EXTRACT | Failed with error: {str(e)}")
        raise