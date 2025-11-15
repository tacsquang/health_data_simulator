import pandas as pd
import json
import time
import numpy as np
from confluent_kafka import Producer
import sys
import os

# --- 0. DEFINE PATHS ---
try:
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
except NameError:
    BASE_DIR = os.path.abspath('.')

CONFIG_PATH = os.path.join(BASE_DIR, 'config', 'kafka_config.json')
DATA_DIR = os.path.join(BASE_DIR, 'data_set') 
LOG_DIR = os.path.join(BASE_DIR, 'logs')
LOG_FILE_PATH = os.path.join(LOG_DIR, 'kafka_simulation_log.jsonl')

SIMULATION_SPEED = 0.05 

# --- 1. LOAD KAFKA CONFIGURATION ---
print(f"INFO: Loading configuration from {CONFIG_PATH}")
try:
    with open(CONFIG_PATH, 'r') as f:
        config = json.load(f)
except FileNotFoundError:
    print(f"ERROR: Configuration file not found at {CONFIG_PATH}", file=sys.stderr)
    print("ERROR: Please create config/kafka_config.json with your API keys.", file=sys.stderr)
    sys.exit(1)
except json.JSONDecodeError:
    print(f"ERROR: File {CONFIG_PATH} is not valid JSON.", file=sys.stderr)
    sys.exit(1)
    
try:
    producer = Producer(config)
    print("INFO: Kafka Producer connected.")
except Exception as e:
    print(f"ERROR: Kafka Producer connection failed: {e}", file=sys.stderr)
    sys.exit(1)


# --- 2. DEFINE DATA SOURCES ---
REGULAR_TOPICS = [
    {
        'file': os.path.join(DATA_DIR, 'user_changes_history.csv'),
        'topic': 'user_cdc',
        'time_col': 'timestamp',
        'event_type': 'user_change'
    },
    {
        'file': os.path.join(DATA_DIR, 'health_data_stream.csv'),
        'topic': 'health_events',
        'time_col': 'event_timestamp',
        'event_type': 'health_snapshot'
    },
    {
        'file': os.path.join(DATA_DIR, 'workout_events.csv'),
        'topic': 'workout_events',
        'time_col': 'timestamp',
        'event_type': 'workout_action'
    }
]
GYM_LOGIN_FILE = os.path.join(DATA_DIR, 'gym_logins_realistic.csv')
GYM_TOPIC = 'gym_sessions'


# --- 3. DATA PROCESSING ---
print("INFO: [1/3] Reading and merging data sources...")
all_records = [] 

# 3.1. Process regular files
for item in REGULAR_TOPICS:
    try:
        print(f"INFO: Reading {item['file']}...")
        df = pd.read_csv(item['file'])
        df = df.replace({np.nan: None})
        df = df.rename(columns={item['time_col']: 'event_timestamp'}) 
        
        clean_records = df.to_dict('records')
        for rec in clean_records:
            rec['_kafka_topic'] = item['topic']
            rec['_event_type'] = item['event_type']
        all_records.extend(clean_records)
        
    except FileNotFoundError:
        print(f"WARNING: Source file not found: {item['file']}", file=sys.stderr)

# 3.2. Process gym_logins file (SPLIT INTO 2 EVENTS)
print(f"INFO: Reading and splitting {GYM_LOGIN_FILE}...")
try:
    df_gym = pd.read_csv(GYM_LOGIN_FILE)
    df_gym = df_gym.replace({np.nan: None})
    
    for row in df_gym.itertuples():
        all_records.append({
            '_kafka_topic': GYM_TOPIC,
            '_event_type': 'gym_check_in',
            'event_timestamp': row.login_ts,
            'user_id': row.user_id,
            'gym_id': row.gym_id,
            'action': 'login'
        })
        all_records.append({
            '_kafka_topic': GYM_TOPIC,
            '_event_type': 'gym_check_out',
            'event_timestamp': row.logout_ts,
            'user_id': row.user_id,
            'gym_id': row.gym_id,
            'action': 'logout'
        })
except FileNotFoundError:
    print(f"WARNING: Source file not found: {GYM_LOGIN_FILE}", file=sys.stderr)

if not all_records:
    print("ERROR: No data found to send. Check the 'output_data/' directory.", file=sys.stderr)
    sys.exit(1)

# 3.3. Sort events
print(f"INFO: [2/3] Merged {len(all_records)} total events. Sorting...")
all_records.sort(key=lambda x: x['event_timestamp'])
print("INFO: [2/3] Sorting complete.")


# --- 4. PRODUCE TO KAFKA & WRITE LOGS ---
print(f"INFO: [3/3] Starting stream. Logging simulation to {LOG_FILE_PATH}...")
os.makedirs(LOG_DIR, exist_ok=True)
count = 0

try:
    with open(LOG_FILE_PATH, "w", encoding="utf-8") as log_file:
        
        for rec in all_records:
            topic = rec.pop('_kafka_topic')
            event_type = rec.pop('_event_type')
            timestamp = rec['event_timestamp'] 
            user_key = str(rec.get('user_id', 'unknown'))
            
            value_json_string = json.dumps(rec)
            value_bytes = value_json_string.encode('utf-8')
            
            # Write to log file
            log_entry = {
                "timestamp": timestamp,
                "topic": topic,
                "key": user_key,
                "value": value_json_string
            }
            log_file.write(json.dumps(log_entry) + "\n")

            # Produce to Kafka
            producer.produce(topic, key=user_key, value=value_bytes)
            
            producer.poll(0)
            time.sleep(SIMULATION_SPEED)
            
            count += 1
            if count % 500 == 0:
                # Log progress
                print(f"INFO: Sent {count} messages... (Last event: {event_type})")

    producer.flush()
    print(f"\nINFO: COMPLETE. Total {count} messages were sent and logged.")

except KeyboardInterrupt:
    print("\nINFO: User interruption detected. Flushing remaining messages...")
    producer.flush()
except Exception as e:
    print(f"\nERROR: Exception during streaming: {e}", file=sys.stderr)
    producer.flush()