import json
import random
import time
from datetime import datetime
import boto3

# Configuration
S3_BUCKET_NAME = "your-telemetry-raw-bucket"  # Change to your bucket
AWS_REGION = "us-east-1"

s3_client = boto3.client('s3', region_name=AWS_REGION)

DEVICE_IDS = [f"dev-{i:03d}" for i in range(1, 11)]

def generate_telemetry_event():
    return {
        "event_id": f"evt-{random.randint(100000, 999999)}",
        "device_id": random.choice(DEVICE_IDS),
        "temperature": round(random.uniform(18.0, 95.0), 2),
        "speed": round(random.uniform(0.0, 120.0), 2),
        "status": random.choice(["OK", "OK", "OK", "WARNING", "CRITICAL"]),
        "timestamp": datetime.utcnow().isoformat()
    }

def main():
    print(f"Starting producer... Writing events to s3://{S3_BUCKET_NAME}/events/")
    try:
        while True:
            # Generate a batch of 5 events
            events = [generate_telemetry_event() for _ in range(5)]
            file_name = f"events/telemetry_{int(time.time())}.json"
            
            # Save multi-line JSON or NDJSON format
            payload = "\n".join([json.dumps(e) for e in events])
            
            s3_client.put_object(
                Bucket=S3_BUCKET_NAME,
                Key=file_name,
                Body=payload.encode('utf-8')
            )
            print(f"Successfully uploaded {file_name}")
            time.sleep(3)  # Push a file every 3 seconds
    except KeyboardInterrupt:
        print("Producer stopped.")

if __name__ == "__main__":
    main()