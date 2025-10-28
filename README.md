# Spotify Music Analytics Platform

A comprehensive data analytics platform that tracks your Spotify listening habits and provides insights through automated ETL pipelines and data visualization.

## What This Project Does

This platform automatically:
- **Collects** your Spotify listening history via Spotify Web API
- **Processes** the data through a robust ETL pipeline (MongoDB → PostgreSQL)
- **Analyzes** your music preferences, listening patterns, and trends
- **Stores** everything in a scalable data warehouse for advanced analytics

## Architecture Overview

```
Spotify API → MongoDB (Raw Data) → Airflow ETL → PostgreSQL (Analytics) → Dashboards
```

- **Data Collection**: Automated Spotify API calls to gather listening history
- **Raw Storage**: MongoDB for flexible JSON document storage
- **ETL Pipeline**: Airflow DAG for daily data processing and transformation
- **Analytics Storage**: PostgreSQL data warehouse with star schema design
- **Insights**: Ready for BI tools (Tableau, Power BI, Grafana)

## Quick Start

### Prerequisites
- Python 3.11+
- Docker (optional, for easier setup)
- Spotify Premium account
- MongoDB Atlas account (free tier)
- Supabase account (free tier)

### 1. Clone and Setup
```bash
git clone <repository-url>
cd spotify-music-analytics

# Setup environment
make dev-start  # One-command setup: venv, dependencies, airflow

# Or manual setup:
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configure Environment
```bash
# Copy environment template
cp .env.example .env

# Edit .env with your credentials:
# - Spotify API credentials (Client ID, Client Secret)
# - MongoDB Atlas connection string
# - Supabase PostgreSQL URL
```

### 3. Setup Databases
```bash
# Test connections
make db-test

# Initialize database schemas
python scripts/init_database.py
```

### 4. Start Airflow
```bash
# Start Airflow webserver and scheduler
make airflow-start

# Open http://localhost:8080
# Default credentials: admin/admin
```

### 5. Run ETL Pipeline
1. Go to Airflow UI (http://localhost:8080)
2. Enable the `daily_etl_pipeline` DAG
3. Trigger manually or wait for scheduled run (2 AM daily)

## Project Structure

```
spotify-music-analytics/
├── dags/spotify/              # Airflow DAG definitions
│   ├── daily_etl_pipeline.py  # Main ETL pipeline
│   └── spotify_collector.py   # Data collection DAG
├── utils/                     # Shared utilities
│   ├── database.py           # Database connections
│   ├── config.py             # Configuration management
│   └── spotify_api.py        # Spotify API client
├── sql/ddl/                  # Database schema definitions
├── scripts/                  # Setup and utility scripts
├── tests/                    # Unit and integration tests
├── requirements.txt          # Python dependencies
├── Makefile                  # Development commands
└── README.md                # This file
```

## Development

### Available Commands
```bash
make help                     # Show all available commands
make dev-start               # Complete development setup
make airflow-start           # Start Airflow
make airflow-logs            # View Airflow logs
make db-test                 # Test database connections
make spotify-test            # Test Spotify API
make clean-all               # Clean temporary files
```

### Running Tests
```bash
# Run all tests
pytest

# Run specific test categories
pytest tests/unit/
pytest tests/integration/
```

### Adding New Features
1. **New Data Sources**: Add collectors in `dags/spotify/`
2. **Database Changes**: Update schemas in `sql/ddl/`
3. **New Analysis**: Create new DAGs or modify existing ones
4. **Utilities**: Add shared functions to `utils/`

## ETL Pipeline Details

### Daily ETL Pipeline (`daily_etl_pipeline`)
Runs every day at 2 AM and processes:

1. **Data Sync**: MongoDB → PostgreSQL raw staging
2. **Data Cleaning**: Time analysis and quality checks
3. **Dimension Loading**: Artists, albums, tracks reference data
4. **Fact Loading**: Core listening events
5. **Aggregation**: Daily/weekly statistics
6. **Reporting**: Execution summaries and data quality metrics

### Data Flow
```
MongoDB Collections:
├── daily_listening_history    # Raw listening events
├── track_details             # Song metadata
├── artist_profiles           # Artist information
└── album_catalog            # Album details

PostgreSQL Schemas:
├── raw_staging              # Direct MongoDB sync
├── clean_staging            # Processed and validated
└── dwh                      # Star schema warehouse
    ├── dim_tracks           # Song dimensions
    ├── dim_artists          # Artist dimensions
    ├── dim_albums           # Album dimensions
    ├── dim_dates            # Date dimensions
    └── fact_listening       # Listening events
```

## Configuration

### Environment Variables (.env)
```env
# Spotify API
SPOTIFY_CLIENT_ID=your_client_id
SPOTIFY_CLIENT_SECRET=your_client_secret
SPOTIFY_REFRESH_TOKEN=your_refresh_token

# MongoDB Atlas
MONGODB_ATLAS_CONNECTION_STRING=mongodb+srv://...
MONGODB_ATLAS_DB_NAME=music_data

# PostgreSQL (Supabase)
SUPABASE_URL=postgresql://...
SUPABASE_DB_NAME=postgres

# Airflow
AIRFLOW_HOME=/path/to/project
```

### Spotify API Setup
1. Go to [Spotify Developer Dashboard](https://developer.spotify.com/dashboard/)
2. Create new app
3. Get Client ID and Client Secret
4. Set redirect URI to `http://localhost:8080/callback`
5. Follow OAuth flow to get refresh token

## Analytics & Insights

The platform enables analysis of:
- **Listening Patterns**: Daily/weekly/monthly trends
- **Music Preferences**: Favorite artists, songs, genres
- **Time Analysis**: When you listen to different types of music
- **Discovery Metrics**: New vs. repeat listening
- **Audio Features**: Energy, valence, danceability trends

### Example Queries
```sql
-- Daily listening summary
SELECT * FROM dwh.v_today_listening;

-- Top artists this month
SELECT artist_name, SUM(play_count) as total_plays
FROM dwh.fact_listening f
JOIN dwh.dim_artists a ON f.artist_key = a.artist_key
JOIN dwh.dim_dates d ON f.date_key = d.date_key
WHERE d.month_name = 'October' AND d.year = 2025
GROUP BY artist_name
ORDER BY total_plays DESC
LIMIT 10;
```

## Troubleshooting

### Common Issues
1. **Airflow won't start**: Check if port 8080 is available
2. **Database connection fails**: Verify credentials in `.env`
3. **Spotify API errors**: Check token expiration and rate limits
4. **DAG import errors**: Ensure all dependencies are installed

### Debug Commands
```bash
# Check Airflow status
make airflow-status

# Test individual components
make db-test
make spotify-test

# View detailed logs
make airflow-logs
```

## Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/new-feature`
3. Make changes and test thoroughly
4. Submit a pull request with clear description

### Development Guidelines
- Follow PEP 8 for Python code style
- Add tests for new functionality
- Update documentation for significant changes
- Use meaningful commit messages

## License

MIT License - see LICENSE file for details.

## Support

- **Issues**: Create GitHub issues for bugs or feature requests
- **Discussions**: Use GitHub Discussions for questions
- **Documentation**: Check wiki for detailed guides

---

**Built with ❤️ for music lovers and data enthusiasts**
