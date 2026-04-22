from flask import Flask, render_template, jsonify, request
from flask.json.provider import DefaultJSONProvider
import pandas as pd
import os
import logging
import subprocess
import json
import sys
from flask import session
from src.auth.rbac import has_permission
from src.auth.models import create_tables, seed_data
from src.auth.auth_service import create_user
from flask import session
from src.auth.rbac import has_permission

# Custom JSON Provider for Flask 3.x to handle NaN and NumPy types
class CricketJSONProvider(DefaultJSONProvider):
    def dumps(self, obj, **kwargs):
        def clean_obj(inner_obj):
            if isinstance(inner_obj, dict):
                return {k: clean_obj(v) for k, v in inner_obj.items()}
            if isinstance(inner_obj, list):
                return [clean_obj(i) for i in inner_obj]
            if hasattr(inner_obj, 'tolist'):
                return inner_obj.tolist()
            if pd.isna(inner_obj):
                return None
            return inner_obj
        
        return super().dumps(clean_obj(obj), **kwargs)

app = Flask(__name__)

app.secret_key = "secret123"  # needed for session

create_tables()
seed_data()

# create test users
create_user("admin_user", "1234", "admin")
create_user("normal_user", "1234", "user")
create_user("engineer_user", "1234", "engineer")

@app.route("/login/<int:user_id>")
def login(user_id):
    session["user_id"] = user_id
    return f"Logged in as user {user_id}"


app.json = CricketJSONProvider(app)

# Setup logging
log_file = os.path.join(os.path.dirname(__file__), '..', 'logs', 'web.log')
if not os.path.exists(os.path.dirname(log_file)):
    os.makedirs(os.path.dirname(log_file))
logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger()

# Global Dataframes
matches_df = pd.DataFrame()
deliveries_df = pd.DataFrame()
player_stats_df = pd.DataFrame()
team_stats_df = pd.DataFrame()
match_summary_df = pd.DataFrame()
merged_df = pd.DataFrame()

# Load data
data_dir = os.path.join(os.path.dirname(__file__), '..', 'data', 'processed')

def load_data():
    global matches_df, deliveries_df, player_stats_df, team_stats_df, match_summary_df, merged_df
    try:
        matches_df = pd.read_csv(os.path.join(data_dir, 'matches.csv'))
        deliveries_df = pd.read_csv(os.path.join(data_dir, 'deliveries.csv'))
        player_stats_df = pd.read_csv(os.path.join(data_dir, 'player_stats.csv'))
        team_stats_df = pd.read_csv(os.path.join(data_dir, 'team_stats.csv'))
        match_summary_df = pd.read_csv(os.path.join(data_dir, 'match_summary.csv'))

        # Ensure date is datetime for filtering
        if 'date' in matches_df.columns:
            matches_df['date'] = pd.to_datetime(matches_df['date'])

        # Merge for easier analytics
        if not matches_df.empty and not deliveries_df.empty:
            merged_df = pd.merge(deliveries_df, matches_df[['match_id', 'team1', 'team2', 'date', 'venue', 'winner']], on='match_id', how='left')
            merged_df['bowling_team'] = merged_df.apply(
                lambda row: row['team2'] if row['batting_team'] == row['team1'] else row['team1'], axis=1
            )
        
        logger.info(f"Data loaded: {len(matches_df)} matches, {len(deliveries_df)} deliveries, {len(merged_df)} merged rows")
    except Exception as e:
        logger.error(f"Error loading data: {e}")

load_data()

@app.route('/api/log_interaction', methods=['POST'])
def log_interaction():
    data = request.json or {}
    action = data.get('action', 'Unknown')
    metadata = data.get('metadata', {})
    ip = request.remote_addr
    logger.info(f"USER_INTERACTION | IP: {ip} | ACTION: {action} | METADATA: {json.dumps(metadata)}")
    return jsonify({'status': 'success'})

@app.route('/')
def index():
    logger.info("Home page accessed")
    return render_template('index.html')

@app.route('/admin')
def admin():
    user_id = session.get("user_id")

    if not user_id or not has_permission(user_id, "run_pipeline"):
        return "Access Denied"

    logger.info(f"Admin page accessed by user {user_id}")
    return render_template('admin.html')

@app.route('/api/filter_options')
def filter_options():
    if matches_df.empty:
        return jsonify({'teams': [], 'players': [], 'venues': [], 'date_range': {'min': '', 'max': ''}})
        
    teams = pd.concat([matches_df['team1'], matches_df['team2']]).unique().tolist()
    players = deliveries_df['batter'].unique().tolist()
    venues = matches_df['venue'].unique().tolist()
    # Format dates as strings for JSON/Date Inputs
    date_min = matches_df['date'].min().strftime('%Y-%m-%d')
    date_max = matches_df['date'].max().strftime('%Y-%m-%d')
    
    return jsonify({
        'teams': sorted(teams),
        'players': sorted(players),
        'venues': sorted(venues),
        'date_range': {'min': date_min, 'max': date_max}
    })

@app.route('/api/filter_players')
def filter_players():
    team_a = request.args.get('team_a', 'All')
    team_b = request.args.get('team_b', 'All')
    
    if team_a == 'All' and team_b == 'All':
        players = deliveries_df['batter'].unique().tolist()
    else:
        teams = []
        if team_a != 'All': teams.append(team_a)
        if team_b != 'All': teams.append(team_b)
        players = deliveries_df[deliveries_df['batting_team'].isin(teams)]['batter'].unique().tolist()
    
    return jsonify(sorted(players))

def apply_filters(df, filters):
    if df.empty: return df
    filtered_df = df.copy()
    
    team_a = filters.get('team_a') or filters.get('team') # Backward compatibility
    team_b = filters.get('team_b')

    if team_a and team_a != 'All' and team_b and team_b != 'All':
        # Head-to-Head mode
        filtered_df = filtered_df[
            ((filtered_df['team1'] == team_a) & (filtered_df['team2'] == team_b)) |
            ((filtered_df['team1'] == team_b) & (filtered_df['team2'] == team_a))
        ]
    elif team_a and team_a != 'All':
        # Single team mode
        filtered_df = filtered_df[(filtered_df['team1'] == team_a) | (filtered_df['team2'] == team_a)]
    
    if filters.get('venue') and filters['venue'] != 'All':
        filtered_df = filtered_df[filtered_df['venue'] == filters['venue']]
    if filters.get('date_start'):
        filtered_df = filtered_df[filtered_df['date'] >= filters['date_start']]
    if filters.get('date_end'):
        filtered_df = filtered_df[filtered_df['date'] <= filters['date_end']]
    return filtered_df

@app.route('/api/dashboard_stats', methods=['POST'])
def dashboard_stats():
    filters = request.json or {}
    ip = request.remote_addr
    logger.info(f"API_REQUEST | IP: {ip} | ENDPOINT: /api/dashboard_stats | FILTERS: {json.dumps(filters)}")
    
    f_matches = apply_filters(matches_df, filters)
    match_ids = f_matches['match_id'].unique()
    
    f_deliveries = merged_df[merged_df['match_id'].isin(match_ids)]
    if filters.get('player') and filters['player'] != 'All':
        f_deliveries = f_deliveries[f_deliveries['batter'] == filters['player']]
    
    if filters.get('over_min') or filters.get('over_max'):
        omin = float(filters.get('over_min', 0))
        omax = float(filters.get('over_max', 50))
        f_deliveries = f_deliveries[(f_deliveries['over'] >= omin) & (f_deliveries['over'] <= omax)]

    total_matches = len(match_ids)
    total_runs = f_deliveries['runs_total'].sum()
    total_wickets = f_deliveries['wicket_kind'].notna().sum()
    avg_score = total_runs / (total_matches * 2) if total_matches > 0 else 0

    # Top Scorers in this filter
    top_scorers = f_deliveries.groupby('batter')['runs_total'].sum().sort_values(ascending=False).head(5).reset_index()
    top_scorers = top_scorers.to_dict(orient='records')

    # Top Wicket Takers
    top_bowlers = f_deliveries.groupby('bowler')['wicket_kind'].count().sort_values(ascending=False).head(5).reset_index()
    top_bowlers = top_bowlers.to_dict(orient='records')

    # Team wins
    team_wins = f_matches['winner'].value_counts().head(1)
    most_wins_team = team_wins.index[0] if not team_wins.empty else "N/A"
    most_wins_count = int(team_wins.values[0]) if not team_wins.empty else 0

    # Milestone Tracker
    batter_scores = f_deliveries.groupby(['match_id', 'batter'])['runs_total'].sum()
    fifties = int((batter_scores >= 50).sum())
    centuries = int((batter_scores >= 100).sum())

    # Highest score
    match_scores = f_deliveries.groupby(['match_id', 'batting_team'])['runs_total'].sum()
    highest_score = int(match_scores.max()) if not match_scores.empty else 0
    highest_score_team = match_scores.idxmax()[1] if not match_scores.empty else "N/A"

    # Prepare safe dicts
    def to_safe_list(df):
        return df.where(pd.notnull(df), None).to_dict(orient='records')

    # Format matches and dates
    recent_matches_df = f_matches.sort_values('date', ascending=False).head(5).copy()
    if not recent_matches_df.empty and 'date' in recent_matches_df.columns:
        recent_matches_df['date'] = recent_matches_df['date'].dt.strftime('%Y-%m-%d')

    return jsonify({
        'kpis': {
            'total_matches': int(total_matches),
            'total_runs': int(total_runs),
            'total_wickets': int(total_wickets),
            'avg_score': float(round(avg_score, 2))
        },
        'top_scorers': to_safe_list(f_deliveries.groupby('batter')['runs_total'].sum().sort_values(ascending=False).head(5).reset_index()),
        'top_bowlers': to_safe_list(f_deliveries.groupby('bowler')['wicket_kind'].count().sort_values(ascending=False).head(5).reset_index()),
        'insights': {
            'most_wins_team': str(most_wins_team),
            'most_wins_count': int(most_wins_count),
            'highest_score': int(highest_score),
            'highest_score_team': str(highest_score_team),
            'fifties': fifties,
            'centuries': centuries
        },
        'recent_matches': to_safe_list(recent_matches_df)
    })

@app.route('/api/charts/runs_distribution', methods=['POST'])
def runs_distribution():
    filters = request.json or {}
    f_matches = apply_filters(matches_df, filters)
    match_ids = f_matches['match_id'].unique()
    f_deliveries = merged_df[merged_df['match_id'].isin(match_ids)]
    
    # Handle player-specific distribution
    if filters.get('player') and filters['player'] != 'All':
        opp_dist = f_deliveries.groupby('bowling_team')['runs_total'].sum().sort_values(ascending=False).head(5)
        return jsonify(opp_dist.to_dict())

    # Distribution by phases
    phases = {
        'Powerplay (0-6)': int(f_deliveries[f_deliveries['over'] < 6]['runs_total'].sum()),
        'Middle (6-15)': int(f_deliveries[(f_deliveries['over'] >= 6) & (f_deliveries['over'] < 15)]['runs_total'].sum()),
        'Death (15-20)': int(f_deliveries[f_deliveries['over'] >= 15]['runs_total'].sum())
    }
    return jsonify(phases)

@app.route('/api/charts/player_trends', methods=['POST'])
def player_trends():
    filters = request.json or {}
    player = filters.get('player')
    if not player or player == 'All':
        if not merged_df.empty:
            player = merged_df.groupby('batter')['runs_total'].sum().idxmax()
        else:
            return jsonify({'player': 'N/A', 'dates': [], 'runs': []})
    
    player_data = merged_df[merged_df['batter'] == player].sort_values('date')
    trends = player_data.groupby('date')['runs_total'].sum().reset_index()
    return jsonify({
        'player': player,
        'dates': [d.strftime('%Y-%m-%d') if hasattr(d, 'strftime') else str(d) for d in trends['date'].tolist()],
        'runs': [int(r) for r in trends['runs_total'].tolist()]
    })

# Admin Endpoints
@app.route('/api/admin/users')
def admin_users():
    return jsonify([
        {'id': 1, 'name': 'Liam Henderson', 'email': 'liam.h@precisionpitch.io', 'role': 'Admin', 'last_active': '2 mins ago', 'active': True},
        {'id': 2, 'name': 'Sarah Jenkins', 'email': 's.jenkins@cricketfed.org', 'role': 'Analyst', 'last_active': '14h ago', 'active': True},
        {'id': 3, 'name': 'Marcus Thorne', 'email': 'm.thorne@stats.global', 'role': 'Viewer', 'last_active': '3 days ago', 'active': False}
    ])

@app.route('/api/admin/metrics')
def admin_metrics():
    def get_dir_size(path):
        total = 0
        try:
            for entry in os.scandir(path):
                if entry.is_file(): total += entry.stat().st_size
                elif entry.is_dir(): total += get_dir_size(entry.path)
        except Exception: pass
        return total

    def format_size(bytes):
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if bytes < 1024.0:
                return f"{bytes:.1f} {unit}"
            bytes /= 1024.0
        return f"{bytes:.1f} PB"

    data_path = os.path.join(os.path.dirname(__file__), '..', 'data')
    actual_size = format_size(get_dir_size(data_path))

    return jsonify({
        'total_users': 42891,
        'active_sessions': 1204,
        'queries_24h': '85.2k',
        'data_size': actual_size
    })

@app.route('/api/admin/upload-file', methods=['POST'])
def admin_upload_file():
    if 'file' not in request.files:
        return jsonify({'status': 'error', 'message': 'No file part'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'status': 'error', 'message': 'No selected file'}), 400
    
    if file and file.filename.endswith('.json'):
        upload_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'new')
        if not os.path.exists(upload_path):
            os.makedirs(upload_path)
            
        target_path = os.path.join(upload_path, file.filename)
        file.save(target_path)
        logger.info(f"Admin uploaded new file: {file.filename}")
        return jsonify({'status': 'success', 'message': 'File uploaded successfully. ETL can now be run.'})
    
    return jsonify({'status': 'error', 'message': 'Invalid file type. Only JSON allowed.'}), 400

@app.route('/api/admin/upload', methods=['POST'])
def admin_trigger_etl():
    # 🔐 RBAC CHECK (ADD THIS BLOCK)
    user_id = session.get("user_id")

    if not user_id or not has_permission(user_id, "run_pipeline"):
        logger.warning(f"Unauthorized ETL attempt by user {user_id}")
        return jsonify({'status': 'error', 'message': 'Access Denied'}), 403

    # EXISTING CODE (KEEP SAME BELOW)
    # Check if there are new files to process
    new_data_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'new')
    has_new_files = os.path.exists(new_data_path) and len(os.listdir(new_data_path)) > 0
    
    if not has_new_files:
        return jsonify({'status': 'ok', 'message': 'All is okay. No new data to process.'})

    logger.info(f"User {user_id} triggered ETL pipeline execution...")

    try:
        root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        result = subprocess.run(
            [sys.executable, 'src/main.py'],
            cwd=root_dir,
            capture_output=True,
            text=True,
            check=True
        )
        
        load_data() 
        return jsonify({
            'status': 'success',
            'message': 'ETL Pipeline completed successfully',
            'output': result.stdout
        })

    except subprocess.CalledProcessError as e:
        logger.error(f"ETL Pipeline failed: {e.stderr}")
        return jsonify({
            'status': 'error',
            'message': 'ETL Pipeline failed',
            'error': e.stderr
        }), 500

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000, debug=True)