import pandas as pd
import os

def save_to_csv(matches, deliveries, player_stats, team_stats, match_summary, output_path, output_config, logger):
    os.makedirs(output_path, exist_ok=True)

    matches_df = pd.DataFrame(matches)
    deliveries_df = pd.DataFrame(deliveries)

    # Save and log row counts
    m_path = os.path.join(output_path, output_config.get("matches_file", "matches.csv"))
    matches_df.to_csv(m_path, index=False)
    logger.info(f"LOAD | FILE: {os.path.basename(m_path)} | ROWS: {len(matches_df)}")

    d_path = os.path.join(output_path, output_config.get("deliveries_file", "deliveries.csv"))
    deliveries_df.to_csv(d_path, index=False)
    logger.info(f"LOAD | FILE: {os.path.basename(d_path)} | ROWS: {len(deliveries_df)}")

    p_path = os.path.join(output_path, "player_stats.csv")
    player_stats.to_csv(p_path, index=False)
    logger.info(f"LOAD | FILE: player_stats.csv | ROWS: {len(player_stats)}")

    t_path = os.path.join(output_path, "team_stats.csv")
    team_stats.to_csv(t_path, index=False)
    logger.info(f"LOAD | FILE: team_stats.csv | ROWS: {len(team_stats)}")

    ms_path = os.path.join(output_path, "match_summary.csv")
    match_summary.to_csv(ms_path, index=False)
    logger.info(f"LOAD | FILE: match_summary.csv | ROWS: {len(match_summary)}")

    logger.info(f"LOAD | STATUS: Success | PATH: {output_path}")