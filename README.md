
# Cricsheet ETL Pipeline (Women’s World Cup)

This repository contains a modular ETL pipeline built using Python and Pandas to process ball-by-ball cricket data from Cricsheet.  
The pipeline ingests raw JSON match files, transforms deeply nested cricket data into well-structured datasets, and enables downstream analytics on player and match performance.

***

## Features

*   Extracts and processes Cricsheet JSON datasets provided in ZIP format
*   Transforms nested cricket data structures (innings → overs → deliveries) into flat, analysis-ready tables
*   **Precision Pitch Dashboard**: Material Design 3 inspired interface with real-time KPIs and advanced filtering
*   **Admin Data Management**: Secure file upload, dynamic folder-size metrics, and ETL pipeline orchestration
*   **Interaction Tracking**: Comprehensive user audit logging for all dashboard and administrative actions
*   Implements a modular ETL architecture with clear separation of Extract, Transform, and Load stages
*   Uses configuration-driven execution through YAML files
*   Provides logging support for pipeline observability and monitoring
*   Dockerized to ensure reproducibility and environment consistency

***

## Project Structure

    cricsheet-etl/
    │
    ├── config/
    │   └── config.yaml
    │
    ├── data/
    │   ├── raw/
    │   └── processed/
    │
    ├── src/
    │   ├── extract.py
    │   ├── transform.py
    │   ├── load.py
    │   ├── utils.py
    │   └── main.py
    │
    ├── analytics/
    │   └── analysis.py
    │
    ├── web/
    │   ├── app.py
    │   ├── static/
    │   │   ├── css/
    │   │   │   └── styles.css
    │   │   └── js/
    │   │       └── script.js
    │   └── templates/
    │       └── index.html
    │
    ├── logs/
    │   ├── etl.log
    │   └── web.log
    │
    ├── Dockerfile
    ├── requirements.txt
    └── README.md

***

## Pipeline Overview

    ZIP File
       ↓
    Extract (unzip)
       ↓
    Transform (flatten JSON)
       ↓
    Load (CSV output)
       ↓
    Analytics

***

## Output Data

### 1. Matches Dataset (`matches.csv`)

Contains match-level metadata, including:

*   Teams
*   Venue
*   Winner
*   Match date

### 2. Deliveries Dataset (`deliveries.csv`)

Contains ball-by-ball information, including:

*   Batter and bowler details
*   Runs scored
*   Innings, over, and ball numbers

***

## Sample Analytics

The analytics notebook includes examples such as:

*   Top run scorers across matches
*   Best bowlers based on economy rate
*   Strike rate analysis
*   Match-level performance summaries

Refer to `analytics/analysis.py` for details.

***

## Dashboard Capabilities

The **Precision Pitch** dashboard provides high-fidelity analytics:

### 1. Match Intelligence
- **Total Matches**: Real-time count of processed fixtures.
- **Top Performers**: Dynamic tables for leading run-scorers and wicket-takers.
- **Milestones**: Tracking for fifties, centuries, and highest individual scores.

### 2. Advanced Filtering
- **Head-to-Head Mode**: Analyze specific rivalries (e.g., India vs Australia).
- **Venue & Time**: Filter by stadium or custom date ranges.
- **Micro-Analysis**: Filter by player name or specific over ranges (0-50).

### 3. Visual Analytics
- **Phase Distribution**: Runs broken down by Powerplay (0-6), Middle (6-15), and Death overs (15-20).
- **Player Trends**: Performance progression charts over historical dates.

***

## Admin Data Management

Access the admin portal at `/admin` for system orchestration:

### 1. Data Pipeline
- **Secure Ingestion**: Upload new match JSONs directly to the `data/new/` directory.
- **ETL Trigger**: Execute the full Python transformation pipeline with a single click.
- **Diagnostics**: Real-time log monitoring for extraction and loading stages.

### 2. System Metrics
- **Storage Metrics**: Dynamic calculation of the actual Data folder size on disk.
- **Usage Stats**: Monitoring for active sessions, total users, and query volume in the last 24 hours.

***

## Docker Setup

### Build the Docker Image

    docker build -t womens-icc-etl .

### Run the Pipeline

    docker run -p 5000:5000 womens-icc-etl

***

## Local Setup

### 1. Create a Virtual Environment

    python -m venv venv
    source venv/bin/activate   # macOS/Linux
    venv\Scripts\activate      # Windows

### 2. Install Dependencies

    pip install -r requirements.txt

### 3. Run the Pipeline

    python src/main.py

### 4. Run the Web UI

After the pipeline completes successfully, you can view results through the web interface:

    python web/app.py

Then open your browser and navigate to:

    http://127.0.0.1:5000/

***

## Configuration

All pipeline parameters are defined in the following file:

    config/config.yaml

Example configuration:

```yaml
paths:
  zip_path: "data/icc_womens_cricket_world_cup_json.zip"
  extract_path: "data/raw"
  processed_path: "data/processed"
```

***

## Logging

Logs are written to:

    logs/etl.log

The log file captures:

*   Data extraction status
*   Number of files processed
*   Overall pipeline completion status

***

## Key Learnings

*   Handling and transforming large volumes of nested JSON data
*   Designing modular and maintainable ETL pipelines
*   Applying configuration-driven architecture for flexibility
*   Containerizing data pipelines using Docker
*   Performing analytical transformations and summaries using Pandas

***

## Future Improvements

*   Add database support using PostgreSQL or SQLite
*   Implement data validation and quality checks
*   Introduce unit tests using pytest
*   Support incremental and historical data loads
*   Scale transformations using PySpark for larger datasets

***

## Data Source

Cricsheet: <https://cricsheet.org/>

***
