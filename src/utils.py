import yaml
import logging
import os
import json
import shutil

def load_config(path="config/config.yaml"):
    with open(path, "r") as f:
        return yaml.safe_load(f)

def setup_logger(log_file, level="INFO"):
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    logging.basicConfig(
        filename=log_file,
        level=getattr(logging, level),
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

    return logging.getLogger()
def load_metadata(metadata_path):
    if not os.path.exists(metadata_path):
        return {}
    with open(metadata_path, "r") as f:
        return json.load(f)

def save_metadata(metadata_path, metadata):
    os.makedirs(os.path.dirname(metadata_path), exist_ok=True)
    with open(metadata_path, "w") as f:
        json.dump(metadata, f, indent=4)

def validate_json_file(file_path, logger):
    if not os.path.exists(file_path):
        logger.error(f"VALIDATION | File not found: {file_path}")
        return False

    if os.path.getsize(file_path) == 0:
        logger.error(f"VALIDATION | Empty file: {file_path}")
        return False

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        logger.error(f"VALIDATION | Invalid JSON: {file_path} | ERROR: {str(e)}")
        return False

    # Required fields
    if "info" not in data or "innings" not in data:
        logger.error(f"VALIDATION | Missing required fields in {file_path}")
        return False

    return True

def send_alert(message, logger):
    # For now: log + console
    logger.error(f"ALERT: {message}")
    print(f"ALERT: {message}")

def move_file(src, dest_folder):
    os.makedirs(dest_folder, exist_ok=True)
    dest_path = os.path.join(dest_folder, os.path.basename(src))
    shutil.move(src, dest_path)
    return dest_path

def delete_file(file_path):
    if os.path.exists(file_path):
        os.remove(file_path)