import json
import os
from glob import glob

import pandas as pd


def transform_data(extract_path, logger):
    base_files = glob(os.path.join(extract_path, "**", "*.json"), recursive=True)
    new_data_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "new")
    new_files = glob(os.path.join(new_data_path, "**", "*.json"), recursive=True) if os.path.exists(new_data_path) else []

    logger.info("TRANSFORM | SOURCE: Base Dataset | COUNT: %s", len(base_files))
    logger.info("TRANSFORM | SOURCE: Admin Uploads | COUNT: %s", len(new_files))

    files = list(set(base_files + new_files))
    if not files:
        logger.warning("TRANSFORM | No JSON files found in source directories!")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    matches = []
    deliveries = []

    logger.info("TRANSFORM | TOTAL_FILES_QUEUED: %s", len(files))

    for file_path in files:
        with open(file_path, "r", encoding="utf-8") as file_handle:
            data = json.load(file_handle)

        info = data.get("info", {})
        match_id = os.path.basename(file_path).replace(".json", "")

        matches.append(
            {
                "match_id": match_id,
                "date": (info.get("dates") or [None])[0],
                "team1": (info.get("teams") or [None, None])[0],
                "team2": (info.get("teams") or [None, None])[1],
                "venue": info.get("venue"),
                "city": info.get("city"),
                "winner": info.get("outcome", {}).get("winner"),
                "toss_winner": info.get("toss", {}).get("winner"),
                "toss_decision": info.get("toss", {}).get("decision"),
            }
        )

        for innings_index, innings in enumerate(data.get("innings", [])):
            inning_data = innings if isinstance(innings, dict) and "team" in innings else list(innings.values())[0]
            team = inning_data.get("team")

            for over_data in inning_data.get("overs", []):
                over = over_data.get("over")
                for ball_index, delivery in enumerate(over_data.get("deliveries", [])):
                    wicket = delivery.get("wickets", [{}])[0]
                    deliveries.append(
                        {
                            "match_id": match_id,
                            "innings": innings_index + 1,
                            "batting_team": team,
                            "over": over,
                            "ball": ball_index + 1,
                            "batter": delivery.get("batter"),
                            "bowler": delivery.get("bowler"),
                            "runs_total": delivery.get("runs", {}).get("total"),
                            "wicket_kind": wicket.get("kind"),
                            "player_out": wicket.get("player_out"),
                        }
                    )

    logger.info("Transformation complete")

    matches_df = pd.DataFrame(matches)
    deliveries_df = pd.DataFrame(deliveries)

    if deliveries_df.empty:
        logger.warning("TRANSFORM | No delivery data extracted from found files.")
        return matches_df, deliveries_df, pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    player_stats = deliveries_df.groupby("batter").agg({"match_id": "nunique", "runs_total": ["sum", "count"]}).reset_index()
    player_stats.columns = ["player", "matches", "runs", "balls_faced"]
    player_stats["strike_rate"] = (player_stats["runs"] / player_stats["balls_faced"] * 100).round(2)
    player_stats["average"] = (player_stats["runs"] / player_stats["matches"]).round(2)

    fours_sixes = (
        deliveries_df[deliveries_df["runs_total"].isin([4, 6])]
        .groupby(["batter", "runs_total"])
        .size()
        .unstack(fill_value=0)
        .reset_index()
    )
    if 4 not in fours_sixes.columns:
        fours_sixes[4] = 0
    if 6 not in fours_sixes.columns:
        fours_sixes[6] = 0
    fours_sixes = fours_sixes.rename(columns={"batter": "player", 4: "fours", 6: "sixes"})

    player_stats = player_stats.merge(fours_sixes[["player", "fours", "sixes"]], on="player", how="left").fillna(0)
    player_stats[["fours", "sixes", "matches"]] = player_stats[["fours", "sixes", "matches"]].astype(int)
    player_stats["innings"] = player_stats["matches"]
    player_stats = player_stats[
        ["player", "matches", "innings", "runs", "balls_faced", "strike_rate", "average", "fours", "sixes"]
    ]

    team_stats_list = []
    for team in pd.concat([matches_df["team1"], matches_df["team2"]]).dropna().unique():
        team_matches = matches_df[(matches_df["team1"] == team) | (matches_df["team2"] == team)]
        matches_count = len(team_matches)
        wins = (team_matches["winner"] == team).sum()
        losses = matches_count - wins
        win_percentage = round((wins / matches_count * 100), 2) if matches_count > 0 else 0

        team_deliveries = deliveries_df[deliveries_df["batting_team"] == team]
        avg_score = round((team_deliveries["runs_total"].sum() / matches_count), 2) if matches_count > 0 else 0

        opponent_deliveries = deliveries_df[
            (deliveries_df["match_id"].isin(team_matches["match_id"])) & (deliveries_df["batting_team"] != team)
        ]
        avg_conceded = round((opponent_deliveries["runs_total"].sum() / matches_count), 2) if matches_count > 0 else 0

        team_stats_list.append(
            {
                "team": team,
                "matches": matches_count,
                "wins": int(wins),
                "losses": int(losses),
                "win_percentage": win_percentage,
                "avg_score": avg_score,
                "avg_conceded": avg_conceded,
            }
        )

    team_stats = pd.DataFrame(team_stats_list)

    match_summary_list = []
    for _, match in matches_df.iterrows():
        match_id = match["match_id"]
        team1 = match["team1"]
        team2 = match["team2"]

        team1_score = deliveries_df[
            (deliveries_df["match_id"] == match_id) & (deliveries_df["batting_team"] == team1)
        ]["runs_total"].sum()
        team2_score = deliveries_df[
            (deliveries_df["match_id"] == match_id) & (deliveries_df["batting_team"] == team2)
        ]["runs_total"].sum()

        match_summary_list.append(
            {
                "match_id": match_id,
                "team1_score": int(team1_score),
                "team2_score": int(team2_score),
                "winner": match["winner"],
                "margin": int(abs(team1_score - team2_score)),
                "toss_winner": match["toss_winner"],
                "toss_decision": match["toss_decision"],
            }
        )

    match_summary = pd.DataFrame(match_summary_list)

    logger.info("TRANSFORM | Derived tables successfully calculated")
    logger.info(
        "TRANSFORM | STATS | Players: %s | Teams: %s | Match Summaries: %s",
        len(player_stats),
        len(team_stats),
        len(match_summary),
    )

    return matches, deliveries, player_stats, team_stats, match_summary
