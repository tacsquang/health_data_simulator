# Health Data Simulator

Lightweight tool to simulate health/workout events and stream them to Kafka for testing stream-processing pipelines.

Quick start:

- Copy `config/kafka_config.sample.json` to `config/kafka_config.json` and edit with your Kafka settings.
- Create a venv and install dependencies:

```powershell
python -m venv .venv
.\\.venv\\Scripts\\Activate.ps1
python -m pip install -r requirements.txt
```

- Run the producer:

```powershell
python kafka_producer\\producer.py
```

Logs are written to `logs/kafka_simulation_log.jsonl`. Data is sent to Kafka (JSON by default).

Data files included in `data_set/`:

- `user_changes_history.csv`
- `gym_logins_realistic.csv`
- `health_data_stream.csv`
- `workout_events.csv`