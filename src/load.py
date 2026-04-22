import os

import pandas as pd


def save_to_csv(matches, deliveries, player_stats, team_stats, match_summary, output_path, output_config, logger):
    os.makedirs(output_path, exist_ok=True)

    matches_df = pd.DataFrame(matches)
    deliveries_df = pd.DataFrame(deliveries)

    matches_path = os.path.join(output_path, output_config.get("matches_file", "matches.csv"))
    matches_df.to_csv(matches_path, index=False)
    logger.info("LOAD | FILE: %s | ROWS: %s", os.path.basename(matches_path), len(matches_df))

    deliveries_path = os.path.join(output_path, output_config.get("deliveries_file", "deliveries.csv"))
    deliveries_df.to_csv(deliveries_path, index=False)
    logger.info("LOAD | FILE: %s | ROWS: %s", os.path.basename(deliveries_path), len(deliveries_df))

    player_stats_path = os.path.join(output_path, "player_stats.csv")
    player_stats.to_csv(player_stats_path, index=False)
    logger.info("LOAD | FILE: player_stats.csv | ROWS: %s", len(player_stats))

    team_stats_path = os.path.join(output_path, "team_stats.csv")
    team_stats.to_csv(team_stats_path, index=False)
    logger.info("LOAD | FILE: team_stats.csv | ROWS: %s", len(team_stats))

    match_summary_path = os.path.join(output_path, "match_summary.csv")
    match_summary.to_csv(match_summary_path, index=False)
    logger.info("LOAD | FILE: match_summary.csv | ROWS: %s", len(match_summary))

    logger.info("LOAD | STATUS: Success | PATH: %s", output_path)
