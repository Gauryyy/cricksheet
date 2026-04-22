import os

from extract import extract_zip
from load import save_to_csv
from transform import transform_data
from utils import load_config, setup_logger


def run_pipeline():
    os.chdir(os.path.dirname(os.path.dirname(__file__)))

    config = load_config()
    logger = setup_logger(config["logging"]["log_file"], config["logging"]["level"])

    logger.info("ETL PIPELINE | STAGE: Initialization | STATUS: Started")
    logger.info("ETL PIPELINE | WORKING_DIR: %s", os.getcwd())

    logger.info("ETL PIPELINE | STAGE: Extraction | STATUS: Started")
    extract_zip(config["paths"]["zip_path"], config["paths"]["extract_path"], logger)
    logger.info("ETL PIPELINE | STAGE: Extraction | STATUS: Completed")

    logger.info("ETL PIPELINE | STAGE: Transformation | STATUS: Started")
    matches, deliveries, player_stats, team_stats, match_summary = transform_data(
        config["paths"]["extract_path"], logger
    )
    logger.info(
        "ETL PIPELINE | STAGE: Transformation | STATUS: Completed | DATA_LOADED: %s matches, %s deliveries",
        len(matches),
        len(deliveries),
    )

    logger.info("ETL PIPELINE | STAGE: Loading | STATUS: Started")
    save_to_csv(
        matches,
        deliveries,
        player_stats,
        team_stats,
        match_summary,
        config["paths"]["processed_path"],
        config["output"],
        logger,
    )
    logger.info("ETL PIPELINE | STAGE: Loading | STATUS: Completed")
    logger.info("ETL PIPELINE | STATUS: Success | SUMMARY: Pipeline execution finished successfully")


if __name__ == "__main__":
    run_pipeline()
