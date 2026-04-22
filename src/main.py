import os
import glob
import time
from datetime import datetime

from utils import (
    load_config,
    setup_logger,
    load_metadata,
    save_metadata,
    validate_json_file,
    send_alert,
    move_file,
    delete_file
)

from transform import transform_data
from load import save_to_csv


def run_pipeline(trigger="manual"):
    # Change to project root directory
    os.chdir(os.path.dirname(os.path.dirname(__file__)))
    
    config = load_config()

    logger = setup_logger(
        config["logging"]["log_file"],
        config["logging"]["level"]
    )
    
    logger.info(f"ETL TRIGGERED | MODE: {trigger}")
    logger.info("ETL PIPELINE | STAGE: Initialization | STATUS: Started")
    logger.info(f"ETL PIPELINE | WORKING_DIR: {os.getcwd()}")

    # ------------------ SCHEDULING LOGIC ------------------
    if trigger == "scheduled":
        current_hour = datetime.now().hour
        logger.info("SCHEDULER RUN | Checking schedule conditions")

        if current_hour != 2:
            logger.info(f"Not 2 AM yet (current: {current_hour}). Skipping scheduled run.")
            return
    else:
        logger.info("MANUAL RUN | Triggered by admin")

    # ------------------ PATHS ------------------
    metadata_path = "data/processed/metadata.json"
    raw_path = os.path.join("data", "raw")
    new_path = os.path.join("data", "new")
    failed_path = os.path.join("data", "failed")

    # ------------------ METADATA ------------------
    metadata = load_metadata(metadata_path)

    # ------------------ RETRY CONFIG ------------------
    retry_attempts = config["error_handling"]["retry_attempts"]

    # ------------------ MOVE NEW → RAW ------------------
    if os.path.exists(new_path):
        new_files = glob.glob(os.path.join(new_path, "*.json"))

        for file in new_files:
            move_file(file, raw_path)
            logger.info(f"MOVED | NEW → RAW | FILE: {os.path.basename(file)}")

    # ------------------ FILE DETECTION (ONLY RAW) ------------------
    all_files = glob.glob(os.path.join(raw_path, "**", "*.json"), recursive=True)
    all_files = list(set(all_files))

    # ------------------ FILTER ------------------
    files_to_process = []

    for file in all_files:
        file_name = os.path.basename(file)

        if metadata.get(file_name) not in ["SUCCESS", "INVALID"]:
            files_to_process.append(file)

    logger.info(f"FILES | TOTAL: {len(all_files)} | TO_PROCESS: {len(files_to_process)}")

    if not files_to_process:
        logger.info("No new files to process. Skipping ETL.")
        return

    # ------------------ PROCESS EACH FILE ------------------
    for file in files_to_process:
        file_name = os.path.basename(file)

        logger.info(f"PROCESSING FILE: {file_name}")

        # ---------------- VALIDATION ----------------
        if not validate_json_file(file, logger):
            metadata[file_name] = "INVALID"

            send_alert(f"{file_name} is INVALID and deleted", logger)

            delete_file(file)  # remove bad data

            save_metadata(metadata_path, metadata)
            continue

        # ---------------- RETRY LOOP ----------------
        for attempt in range(1, retry_attempts + 1):
            try:
                logger.info(f"RETRY | FILE: {file_name} | ATTEMPT: {attempt}")

                # ---------------- TRANSFORM ----------------
                matches, deliveries, player_stats, team_stats, match_summary = transform_data(
                    file,
                    logger
                )

                # ---------------- LOAD ----------------
                save_to_csv(
                    matches,
                    deliveries,
                    player_stats,
                    team_stats,
                    match_summary,
                    config["paths"]["processed_path"],
                    config["output"],
                    logger
                )

                # ---------------- SUCCESS ----------------
                metadata[file_name] = "SUCCESS"
                logger.info(f"FILE STATUS | {file_name} | SUCCESS")

                break  # success → exit retry loop

            except Exception as e:
                logger.error(f"ERROR | FILE: {file_name} | ATTEMPT: {attempt} | ERROR: {str(e)}")

                if attempt < retry_attempts:
                    wait_time = 2 ** attempt
                    logger.info(f"RETRYING AFTER {wait_time} SECONDS...")
                    time.sleep(wait_time)
                else:
                    metadata[file_name] = "FAILED"

                    move_file(file, failed_path)  # move failed file

                    send_alert(f"{file_name} FAILED after {retry_attempts} attempts and moved to failed/", logger)

        # ---------------- SAVE METADATA ----------------
        save_metadata(metadata_path, metadata)

    logger.info("ETL PIPELINE | STATUS: Completed all files")


if __name__ == "__main__":
    run_pipeline()