import json
import os
import pandas as pd


def transform_data(file_path, logger):
    logger.info(f"TRANSFORM | FILE: {file_path}")

    if not os.path.exists(file_path):
        logger.error(f"TRANSFORM | File not found: {file_path}")
        raise FileNotFoundError(file_path)

    matches = []
    deliveries = []

    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    info = data.get("info", {})
    match_id = os.path.basename(file_path).replace(".json", "")

    # ---------------- MATCH TABLE ----------------
    matches.append({
        "match_id": match_id,
        "date": (info.get("dates") or [None])[0],
        "team1": (info.get("teams") or [None, None])[0],
        "team2": (info.get("teams") or [None, None])[1],
        "venue": info.get("venue"),
        "city": info.get("city"),
        "winner": info.get("outcome", {}).get("winner"),
        "toss_winner": info.get("toss", {}).get("winner"),
        "toss_decision": info.get("toss", {}).get("decision"),
    })

    # ---------------- DELIVERIES ----------------
    for i, innings in enumerate(data.get("innings", [])):

        if isinstance(innings, dict) and "team" in innings:
            inning_data = innings
        else:
            inning_data = list(innings.values())[0]

        team = inning_data.get("team")

        for over_data in inning_data.get("overs", []):
            over = over_data.get("over")

            for ball_idx, delivery in enumerate(over_data.get("deliveries", [])):
                wicket = delivery.get("wickets", [{}])[0]

                deliveries.append({
                    "match_id": match_id,
                    "innings": i + 1,
                    "batting_team": team,
                    "over": over,
                    "ball": ball_idx + 1,
                    "batter": delivery.get("batter"),
                    "bowler": delivery.get("bowler"),
                    "runs_total": delivery.get("runs", {}).get("total"),
                    "wicket_kind": wicket.get("kind"),
                    "player_out": wicket.get("player_out"),
                })

    logger.info("TRANSFORM | Base extraction complete")

    matches_df = pd.DataFrame(matches)
    deliveries_df = pd.DataFrame(deliveries)

    if deliveries_df.empty:
        logger.warning("TRANSFORM | No deliveries found")
        return matches_df, deliveries_df, pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    # ---------------- PLAYER STATS ----------------
    player_stats = deliveries_df.groupby("batter").agg({
        "match_id": "nunique",
        "runs_total": ["sum", "count"]
    }).reset_index()

    player_stats.columns = ["player", "matches", "runs", "balls_faced"]

    player_stats["strike_rate"] = (player_stats["runs"] / player_stats["balls_faced"] * 100).round(2)
    player_stats["average"] = (player_stats["runs"] / player_stats["matches"]).round(2)

    # fours & sixes
    fours_sixes = deliveries_df[deliveries_df["runs_total"].isin([4, 6])] \
        .groupby(["batter", "runs_total"]).size().unstack(fill_value=0).reset_index()

    if 4 not in fours_sixes.columns:
        fours_sixes[4] = 0
    if 6 not in fours_sixes.columns:
        fours_sixes[6] = 0

    fours_sixes.columns = ["player", "fours", "sixes"]

    player_stats = player_stats.merge(fours_sixes, on="player", how="left").fillna(0)

    player_stats[["fours", "sixes", "matches"]] = player_stats[["fours", "sixes", "matches"]].astype(int)
    player_stats["innings"] = player_stats["matches"]

    player_stats = player_stats[
        ["player", "matches", "innings", "runs", "balls_faced", "strike_rate", "average", "fours", "sixes"]
    ]

    # ---------------- TEAM STATS ----------------
    team_stats_list = []

    for team in [matches[0]["team1"], matches[0]["team2"]]:
        team_deliveries = deliveries_df[deliveries_df["batting_team"] == team]

        total_runs = team_deliveries["runs_total"].sum()

        team_stats_list.append({
            "team": team,
            "matches": 1,
            "wins": 1 if matches[0]["winner"] == team else 0,
            "losses": 0 if matches[0]["winner"] == team else 1,
            "win_percentage": 100 if matches[0]["winner"] == team else 0,
            "avg_score": total_runs,
            "avg_conceded": 0  # simplified per-file
        })

    team_stats = pd.DataFrame(team_stats_list)

    # ---------------- MATCH SUMMARY ----------------
    team1 = matches[0]["team1"]
    team2 = matches[0]["team2"]

    team1_score = deliveries_df[
        (deliveries_df["batting_team"] == team1)
    ]["runs_total"].sum()

    team2_score = deliveries_df[
        (deliveries_df["batting_team"] == team2)
    ]["runs_total"].sum()

    match_summary = pd.DataFrame([{
        "match_id": match_id,
        "team1_score": int(team1_score),
        "team2_score": int(team2_score),
        "winner": matches[0]["winner"],
        "margin": abs(int(team1_score - team2_score)),
        "toss_winner": matches[0]["toss_winner"],
        "toss_decision": matches[0]["toss_decision"]
    }])

    logger.info("TRANSFORM | Completed successfully")

    return matches, deliveries, player_stats, team_stats, match_summary