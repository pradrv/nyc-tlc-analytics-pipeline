# Pipeline Monitoring and Scheduling Guide

This guide covers how to monitor and schedule the NYC Taxi & HVFHV Data Pipeline for both local development and production environments.

## Table of Contents

1. [Current State (Local Development)](#current-state-local-development)
2. [Monitoring the Pipeline](#monitoring-the-pipeline)
3. [Scheduling the Pipeline](#scheduling-the-pipeline)
4. [Production-Grade Solutions](#production-grade-solutions)

---

## Current State (Local Development)

### What's Already Built-In

The pipeline currently includes:

**Logging:**
- Structured logging with Loguru
- Log levels: DEBUG, INFO, WARNING, ERROR, SUCCESS
- Logs to console and file (`data/logs/`)

**Basic Monitoring:**
- Database statistics command
- Data quality metrics table
- Ingestion log table

**Orchestration:**
- Prefect flows for task dependencies
- Basic error handling and retries

---

## Monitoring the Pipeline

### 1. Real-Time Log Monitoring

#### Console Logs

**During Pipeline Execution:**
```bash
# Run pipeline and see live logs
uv run python -m src.cli run-e2e --sample

# You'll see:
# 2025-10-06 10:44:16 | INFO     | Starting E2E Pipeline...
# 2025-10-06 10:44:17 | SUCCESS  | Downloaded yellow_tripdata_2024-06.parquet
# 2025-10-06 10:44:18 | INFO     | Loaded 3,404,479 rows to raw_yellow
# 2025-10-06 10:44:20 | SUCCESS  | Quality checks: 98.5% passed
```

**Tail Logs in Real-Time:**
```bash
# In separate terminal, watch log file
tail -f data/logs/pipeline_$(date +%Y-%m-%d).log

# Or use grep to filter
tail -f data/logs/pipeline_*.log | grep ERROR
tail -f data/logs/pipeline_*.log | grep SUCCESS
```

#### Log File Locations

```bash
# Pipeline logs
data/logs/pipeline_2025-10-06.log

# Check recent errors
grep ERROR data/logs/pipeline_*.log | tail -20

# Count warnings by type
grep WARNING data/logs/pipeline_*.log | cut -d'|' -f4 | sort | uniq -c | sort -rn
```

### 2. Database-Level Monitoring

#### Check Pipeline Status

```bash
# Get database statistics
uv run python -m src.cli db-stats

# Output shows:
# - Table row counts
# - Data quality scores
# - Storage size
# - Last update time
```

#### Query Ingestion Logs

```python
# Check what files were ingested
uv run python -c "
import duckdb
conn = duckdb.connect('data/database/nyc_taxi.duckdb')

result = conn.execute('''
    SELECT 
        file_name,
        service_type,
        row_count,
        status,
        ingestion_timestamp
    FROM ingestion_log
    ORDER BY ingestion_timestamp DESC
    LIMIT 10
''').fetchdf()

print(result)
"
```

#### Monitor Data Quality

```python
# Check data quality metrics
uv run python -c "
import duckdb
conn = duckdb.connect('data/database/nyc_taxi.duckdb')

result = conn.execute('''
    SELECT 
        service_type,
        check_type,
        total_rows,
        failed_rows,
        ROUND(failure_rate, 2) as failure_pct,
        check_timestamp
    FROM data_quality_metrics
    ORDER BY check_timestamp DESC
    LIMIT 20
''').fetchdf()

print(result)
"
```

### 3. Create Monitoring Dashboard Script

```bash
# Create monitoring script
cat > scripts/monitor_pipeline.sh << 'EOF'
#!/bin/bash

echo "==============================================="
echo "NYC Taxi Pipeline - Monitoring Dashboard"
echo "==============================================="
echo ""

# 1. Check if database exists
if [ ! -f "data/database/nyc_taxi.duckdb" ]; then
    echo "❌ Database not found!"
    exit 1
fi

echo "✅ Database exists"
echo ""

# 2. Get table statistics
echo "📊 Table Statistics:"
echo "-------------------"
uv run python -c "
import duckdb
conn = duckdb.connect('data/database/nyc_taxi.duckdb')

tables = ['raw_yellow', 'raw_green', 'raw_hvfhv', 'fact_trips']
for table in tables:
    try:
        count = conn.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0]
        print(f'{table:20} {count:>15,} rows')
    except:
        print(f'{table:20} Table not found')
"
echo ""

# 3. Check data quality
echo "✓ Data Quality Summary:"
echo "----------------------"
uv run python -c "
import duckdb
conn = duckdb.connect('data/database/nyc_taxi.duckdb')

try:
    result = conn.execute('''
        SELECT 
            service_type,
            AVG(100 - failure_rate) as avg_quality_pct
        FROM data_quality_metrics
        GROUP BY service_type
    ''').fetchall()
    
    for row in result:
        print(f'{row[0]:15} {row[1]:.1f}% quality')
except:
    print('No quality metrics available')
"
echo ""

# 4. Check recent ingestion
echo "📥 Recent Ingestion (Last 5 files):"
echo "-----------------------------------"
uv run python -c "
import duckdb
conn = duckdb.connect('data/database/nyc_taxi.duckdb')

try:
    result = conn.execute('''
        SELECT 
            file_name,
            status,
            row_count,
            ingestion_timestamp
        FROM ingestion_log
        ORDER BY ingestion_timestamp DESC
        LIMIT 5
    ''').fetchall()
    
    for row in result:
        print(f'{row[0]:40} {row[1]:15} {row[2]:>10,} rows  {row[3]}')
except:
    print('No ingestion logs available')
"
echo ""

# 5. Check disk space
echo "💾 Disk Space:"
echo "-------------"
du -sh data/database/nyc_taxi.duckdb 2>/dev/null || echo "Database file not found"
du -sh data/raw/*.parquet 2>/dev/null | head -5 || echo "No raw files"
echo ""

# 6. Check recent errors
echo "⚠️  Recent Errors (Last 24 hours):"
echo "----------------------------------"
find data/logs -name "*.log" -mtime -1 -exec grep ERROR {} \; 2>/dev/null | tail -5 || echo "No recent errors"
echo ""

echo "==============================================="
echo "Last updated: $(date)"
echo "==============================================="
EOF

chmod +x scripts/monitor_pipeline.sh
```

**Run the monitoring dashboard:**
```bash
./scripts/monitor_pipeline.sh

# Or run continuously
watch -n 30 ./scripts/monitor_pipeline.sh  # Updates every 30 seconds
```

### 4. Email Alerts on Failure

```python
# Create alert script: scripts/alert_on_failure.py
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import sys

def send_alert(subject: str, message: str, to_email: str):
    """Send email alert"""
    from_email = "pipeline@company.com"
    smtp_server = "smtp.gmail.com"
    smtp_port = 587
    password = "your_app_password"  # Use environment variable in production
    
    msg = MIMEMultipart()
    msg['From'] = from_email
    msg['To'] = to_email
    msg['Subject'] = f"[ALERT] NYC Taxi Pipeline - {subject}"
    
    msg.attach(MIMEText(message, 'plain'))
    
    try:
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(from_email, password)
        server.send_message(msg)
        server.quit()
        print(f"Alert sent to {to_email}")
    except Exception as e:
        print(f"Failed to send alert: {e}")

if __name__ == "__main__":
    subject = sys.argv[1] if len(sys.argv) > 1 else "Pipeline Failed"
    message = sys.argv[2] if len(sys.argv) > 2 else "Check logs for details"
    to_email = "data-team@company.com"
    
    send_alert(subject, message, to_email)
```

**Use in pipeline:**
```bash
# Run pipeline with error alerting
uv run python -m src.cli run-e2e --sample || \
  python scripts/alert_on_failure.py "Pipeline Failed" "$(tail -20 data/logs/pipeline_*.log)"
```

### 5. Prefect UI (Built-In)

**Start Prefect UI:**
```bash
# Start Prefect server
./start_prefect_ui.sh

# Or manually:
uv run prefect server start

# Access at: http://localhost:4200
```

**Features:**
- Visual flow execution graph
- Task-level timing and status
- Retry history
- Flow run logs
- Scheduled runs management

---

## Scheduling the Pipeline

### Option 1: Cron (Linux/Mac)

**Simple, Built-In, No Dependencies**

#### Daily Pipeline Run

```bash
# Edit crontab
crontab -e

# Add entry (runs daily at 2 AM)
0 2 * * * cd /Users/pvishwakar005/GDS/data_pipeline && uv run python -m src.cli run-e2e --full >> data/logs/cron_$(date +\%Y-\%m-\%d).log 2>&1

# Explanation:
# 0 2 * * *     - Every day at 2:00 AM
# cd ...        - Change to project directory
# uv run ...    - Run pipeline
# >> ...        - Append output to log file
# 2>&1          - Capture errors too
```

#### Different Schedules

```bash
# Every 6 hours
0 */6 * * * cd /path/to/pipeline && uv run python -m src.cli run-e2e --full

# Every Monday at 3 AM
0 3 * * 1 cd /path/to/pipeline && uv run python -m src.cli run-e2e --full

# First day of every month at 1 AM
0 1 1 * * cd /path/to/pipeline && uv run python -m src.cli run-e2e --full

# Weekdays at 8 AM
0 8 * * 1-5 cd /path/to/pipeline && uv run python -m src.cli run-e2e --sample
```

#### Cron with Error Notifications

```bash
# Create wrapper script: scripts/cron_wrapper.sh
cat > scripts/cron_wrapper.sh << 'EOF'
#!/bin/bash

cd /Users/pvishwakar005/GDS/data_pipeline

LOG_FILE="data/logs/cron_$(date +%Y-%m-%d_%H-%M-%S).log"

echo "Starting pipeline at $(date)" > "$LOG_FILE"

# Run pipeline
if uv run python -m src.cli run-e2e --full >> "$LOG_FILE" 2>&1; then
    echo "Pipeline completed successfully at $(date)" >> "$LOG_FILE"
    
    # Send success notification (optional)
    echo "Pipeline Success: $(date)" | mail -s "NYC Taxi Pipeline Success" data-team@company.com
else
    echo "Pipeline failed at $(date)" >> "$LOG_FILE"
    
    # Send failure notification
    echo "Pipeline failed. Check logs: $LOG_FILE" | mail -s "NYC Taxi Pipeline FAILED" data-team@company.com
    
    # Send Slack notification (if webhook configured)
    curl -X POST -H 'Content-type: application/json' \
         --data "{\"text\":\"Pipeline failed at $(date). Check logs.\"}" \
         https://hooks.slack.com/services/YOUR/WEBHOOK/URL
fi
EOF

chmod +x scripts/cron_wrapper.sh

# Add to crontab
crontab -e
# 0 2 * * * /Users/pvishwakar005/GDS/data_pipeline/scripts/cron_wrapper.sh
```

#### View Cron Logs

```bash
# View cron logs
ls -lh data/logs/cron_*.log

# Tail latest cron log
tail -f data/logs/cron_$(date +%Y-%m-%d)*.log

# Check for errors in cron logs
grep -i error data/logs/cron_*.log
```

### Option 2: Systemd Timer (Linux)

**More Robust than Cron, Better Logging**

#### Create Service File

```bash
# /etc/systemd/system/nyc-taxi-pipeline.service
[Unit]
Description=NYC Taxi Data Pipeline
After=network.target

[Service]
Type=oneshot
User=pvishwakar005
WorkingDirectory=/Users/pvishwakar005/GDS/data_pipeline
Environment="PATH=/usr/local/bin:/usr/bin:/bin"
ExecStart=/usr/local/bin/uv run python -m src.cli run-e2e --full
StandardOutput=append:/Users/pvishwakar005/GDS/data_pipeline/data/logs/systemd_pipeline.log
StandardError=append:/Users/pvishwakar005/GDS/data_pipeline/data/logs/systemd_pipeline_error.log

[Install]
WantedBy=multi-user.target
```

#### Create Timer File

```bash
# /etc/systemd/system/nyc-taxi-pipeline.timer
[Unit]
Description=Run NYC Taxi Pipeline Daily
Requires=nyc-taxi-pipeline.service

[Timer]
OnCalendar=daily
OnCalendar=02:00
Persistent=true

[Install]
WantedBy=timers.target
```

#### Enable and Start

```bash
# Reload systemd
sudo systemctl daemon-reload

# Enable timer (starts on boot)
sudo systemctl enable nyc-taxi-pipeline.timer

# Start timer
sudo systemctl start nyc-taxi-pipeline.timer

# Check timer status
sudo systemctl status nyc-taxi-pipeline.timer

# View scheduled runs
systemctl list-timers

# Manually trigger pipeline
sudo systemctl start nyc-taxi-pipeline.service

# View logs
journalctl -u nyc-taxi-pipeline.service -f
```

### Option 3: Prefect Deployments

**Python-Native, Best for Complex Workflows**

#### Create Deployment Script

```python
# scripts/deploy_to_prefect.py
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule
from src.orchestration.flows import full_pipeline_flow

# Create deployment
deployment = Deployment.build_from_flow(
    flow=full_pipeline_flow,
    name="nyc-taxi-pipeline-daily",
    version="1.0.0",
    work_queue_name="default",
    schedule=CronSchedule(
        cron="0 2 * * *",  # Daily at 2 AM
        timezone="America/New_York"
    ),
    parameters={
        "service_types": ["yellow", "green", "hvfhv"],
        "year_months": None,  # Will use config default
        "skip_download": False
    },
    tags=["production", "etl", "daily"]
)

if __name__ == "__main__":
    deployment.apply()
    print("Deployment created successfully!")
    print("Start agent with: prefect agent start -q default")
```

#### Deploy and Run

```bash
# 1. Start Prefect server
uv run prefect server start &

# 2. Create deployment
uv run python scripts/deploy_to_prefect.py

# 3. Start agent to execute flows
uv run prefect agent start -q default

# 4. View in UI: http://localhost:4200
```

#### Prefect Cloud (Managed)

```bash
# 1. Create account at https://app.prefect.cloud

# 2. Login
uv run prefect cloud login

# 3. Create deployment
uv run python scripts/deploy_to_prefect.py

# 4. Start agent (on your machine or server)
uv run prefect agent start -q default

# Now managed from cloud UI!
```

### Option 4: Mac launchd (Mac-Specific)

**Native Mac Scheduler**

#### Create plist File

```xml
<!-- ~/Library/LaunchAgents/com.nyctaxi.pipeline.plist -->
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.nyctaxi.pipeline</string>
    
    <key>ProgramArguments</key>
    <array>
        <string>/usr/local/bin/uv</string>
        <string>run</string>
        <string>python</string>
        <string>-m</string>
        <string>src.cli</string>
        <string>run-e2e</string>
        <string>--full</string>
    </array>
    
    <key>WorkingDirectory</key>
    <string>/Users/pvishwakar005/GDS/data_pipeline</string>
    
    <key>StartCalendarInterval</key>
    <dict>
        <key>Hour</key>
        <integer>2</integer>
        <key>Minute</key>
        <integer>0</integer>
    </dict>
    
    <key>StandardOutPath</key>
    <string>/Users/pvishwakar005/GDS/data_pipeline/data/logs/launchd_out.log</string>
    
    <key>StandardErrorPath</key>
    <string>/Users/pvishwakar005/GDS/data_pipeline/data/logs/launchd_err.log</string>
    
    <key>RunAtLoad</key>
    <false/>
</dict>
</plist>
```

#### Load and Start

```bash
# Load the job
launchctl load ~/Library/LaunchAgents/com.nyctaxi.pipeline.plist

# Start immediately (for testing)
launchctl start com.nyctaxi.pipeline

# Check status
launchctl list | grep nyctaxi

# View logs
tail -f data/logs/launchd_out.log

# Unload (stop scheduling)
launchctl unload ~/Library/LaunchAgents/com.nyctaxi.pipeline.plist
```

---

## Production-Grade Solutions

### 1. Apache Airflow

**Enterprise-Grade Orchestration**

```python
# dags/nyc_taxi_dag.py
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-engineering',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email': ['alerts@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

with DAG(
    'nyc_taxi_pipeline',
    default_args=default_args,
    description='NYC Taxi ETL Pipeline',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['production', 'etl'],
) as dag:
    
    # Task 1: Run E2E pipeline
    run_pipeline = BashOperator(
        task_id='run_e2e_pipeline',
        bash_command='cd /opt/airflow/dags/data_pipeline && uv run python -m src.cli run-e2e --full',
    )
    
    # Task 2: Check data quality
    check_quality = PythonOperator(
        task_id='check_data_quality',
        python_callable=lambda: check_quality_threshold(min_quality=95.0),
    )
    
    # Task 3: Send success notification
    notify_success = BashOperator(
        task_id='notify_success',
        bash_command='echo "Pipeline completed" | mail -s "Success" team@company.com',
    )
    
    run_pipeline >> check_quality >> notify_success
```

**Install and Run:**
```bash
# Install Airflow
pip install apache-airflow

# Initialize database
airflow db init

# Create user
airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com

# Start webserver
airflow webserver --port 8080

# Start scheduler (in separate terminal)
airflow scheduler

# Access UI: http://localhost:8080
```

### 2. Cloud Managed Solutions

#### AWS (EventBridge + Lambda/ECS)

```python
# AWS EventBridge Schedule
{
    "scheduleExpression": "cron(0 2 * * ? *)",  # Daily 2 AM UTC
    "target": {
        "arn": "arn:aws:ecs:us-east-1:123456789:task-definition/nyc-taxi-pipeline",
        "roleArn": "arn:aws:iam::123456789:role/ecsEventsRole"
    }
}
```

#### Google Cloud (Cloud Scheduler + Cloud Run)

```bash
# Create Cloud Scheduler job
gcloud scheduler jobs create http nyc-taxi-pipeline \
    --schedule="0 2 * * *" \
    --uri="https://nyc-taxi-pipeline-xyz.run.app/trigger" \
    --http-method=POST \
    --time-zone="America/New_York"
```

#### Azure (Logic Apps / Data Factory)

```json
{
    "type": "Recurrence",
    "recurrence": {
        "frequency": "Day",
        "interval": 1,
        "schedule": {
            "hours": [2],
            "minutes": [0]
        },
        "timeZone": "Eastern Standard Time"
    }
}
```

### 3. Monitoring Solutions

#### Prometheus + Grafana

```yaml
# docker-compose.monitoring.yml
version: '3.8'
services:
  prometheus:
    image: prom/prometheus
    ports:
      - "9090:9090"
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml
      
  grafana:
    image: grafana/grafana
    ports:
      - "3000:3000"
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin
```

#### Datadog / New Relic

```python
# Add to pipeline
from datadog import statsd

# Increment counter
statsd.increment('pipeline.runs', tags=['env:prod', 'pipeline:nyc-taxi'])

# Track timing
with statsd.timed('pipeline.duration'):
    run_pipeline()

# Set gauge
statsd.gauge('pipeline.rows_processed', 3404479)
```

---

## Comparison Table

| Solution | Complexity | Cost | Best For | Monitoring UI |
|----------|-----------|------|----------|---------------|
| **Cron** | Low | Free | Simple schedules, single machine | No (logs only) |
| **Systemd Timer** | Low | Free | Linux servers, better than cron | journalctl |
| **launchd** | Low | Free | Mac-only, native scheduler | No (logs only) |
| **Prefect** | Medium | Free (self-hosted) | Python workflows, local dev | Yes (localhost:4200) |
| **Prefect Cloud** | Medium | $0-500/month | Managed Prefect, small teams | Yes (cloud UI) |
| **Airflow** | High | Free (self-hosted) | Complex DAGs, enterprise | Yes (localhost:8080) |
| **Cloud Managed** | Low-Medium | $50-200/month | Cloud-native, scalable | Cloud console |

---

## Recommended Setup by Use Case

### Personal Project / Learning
```bash
✓ Monitoring: ./scripts/monitor_pipeline.sh + Prefect UI
✓ Scheduling: cron (simple and works)
```

### Small Team / Startup
```bash
✓ Monitoring: Prefect Cloud + Basic alerting
✓ Scheduling: Prefect Cloud deployments
✓ Cost: ~$50-100/month
```

### Production / Enterprise
```bash
✓ Monitoring: Airflow UI + Datadog/Grafana
✓ Scheduling: Airflow on Kubernetes
✓ Alerting: PagerDuty + Slack
✓ Cost: $500-2000/month
```

---

## Quick Start Examples

### Example 1: Run Daily with Email Alerts

```bash
# 1. Create alert script
cat > scripts/daily_run_with_alert.sh << 'EOF'
#!/bin/bash
cd /Users/pvishwakar005/GDS/data_pipeline

if uv run python -m src.cli run-e2e --full; then
    echo "Success: $(date)" | mail -s "Pipeline Success" you@email.com
else
    echo "Failed: $(date). Check logs." | mail -s "Pipeline FAILED" you@email.com
fi
EOF

chmod +x scripts/daily_run_with_alert.sh

# 2. Add to crontab
crontab -e
# Add: 0 2 * * * /path/to/scripts/daily_run_with_alert.sh
```

### Example 2: Monitor with Dashboard

```bash
# Run monitoring dashboard every 30 seconds
watch -n 30 './scripts/monitor_pipeline.sh'
```

### Example 3: Prefect Scheduled Flow

```bash
# 1. Start Prefect server
uv run prefect server start &

# 2. Deploy flow
uv run python scripts/deploy_to_prefect.py

# 3. Start agent
uv run prefect agent start -q default &

# 4. Open UI
open http://localhost:4200
```

---

## Troubleshooting

### Cron Not Running

```bash
# Check if cron daemon is running
ps aux | grep cron

# Check cron logs (varies by system)
grep CRON /var/log/syslog  # Ubuntu/Debian
grep CRON /var/log/cron    # CentOS/RHEL

# Test cron entry manually
/bin/bash -c "cd /path && uv run python -m src.cli run-e2e --sample"
```

### Prefect Agent Not Picking Up Flows

```bash
# Check agent is running
ps aux | grep "prefect agent"

# Check work queue
uv run prefect work-queue ls

# Restart agent
pkill -f "prefect agent"
uv run prefect agent start -q default
```

### Pipeline Times Out

```bash
# Increase timeout in cron wrapper
timeout 6h uv run python -m src.cli run-e2e --full

# Or split into smaller chunks
# Run by month instead of full range
```

---

## Additional Resources

- **[README.md](../README.md)** - Project overview
- **[IMPLEMENTATION_GUIDE.md](IMPLEMENTATION_GUIDE.md)** - Detailed implementation
- **Prefect Docs** - https://docs.prefect.io
- **Airflow Docs** - https://airflow.apache.org/docs
- **Cron Expression Generator** - https://crontab.guru

---

**Your pipeline is now ready for monitoring and scheduling!**

