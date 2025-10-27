import base64
import json
import os
from flask import Flask, request
from google.cloud import bigquery

app = Flask(__name__)

# --- Configuration ---
# We get the Project ID and Table ID from environment variables
# set by Cloud Run during deployment.
try:
    PROJECT_ID = os.environ.get("PROJECT_ID")
    TABLE_ID = f"{PROJECT_ID}.my_bq_dataset.customers"
    bq_client = bigquery.Client()
except Exception as e:
    print(f"ERROR: Failed to initialize BigQuery client: {e}")
    bq_client = None

@app.route("/", methods=["POST"])
def index():
    """
    Receives and processes a Pub/Sub push message.
    """
    # Check if the BigQuery client is available.
    if bq_client is None:
        print("ERROR: BigQuery client not initialized. Cannot process message.")
        # Return 200 to Pub/Sub to prevent retries of a bad config.
        return ("BigQuery client not initialized", 200)

    # Get the Pub/Sub message data.
    envelope = request.get_json()
    if not envelope or "message" not in envelope:
        print("ERROR: Bad Request: No Pub/Sub message received")
        return ("Bad Request: No Pub/Sub message received", 400)

    try:
        # Decode the message data from Base64
        pubsub_data = base64.b64decode(envelope["message"]["data"]).decode("utf-8")
        # Parse the Debezium JSON payload
        payload_data = json.loads(pubsub_data)
        
        # Debezium messages have a 'payload' key.
        # If it's missing, it might be a test message.
        if "payload" not in payload_data:
            print(f"WARN: Received message without 'payload' key. Ignoring. Data: {pubsub_data}")
            return ("", 204) # Acknowledge and ignore

        payload = payload_data["payload"]
        op = payload.get("op") # 'c' = create, 'u' = update, 'd' = delete
        
        # For 'c' and 'u', the data is in 'after'. For 'd', it's in 'before'.
        data = payload.get("after") if op != "d" else payload.get("before")

        if not data or not op:
            print(f"WARN: Could not parse 'op' or 'data' from payload. Ignoring. Payload: {payload}")
            return ("", 204) # Acknowledge and ignore

        print(f"Processing operation: '{op}', Data: {data}")

        if op == "d":
            # --- Handle DELETE ---
            run_bq_delete(data)
        else:
            # --- Handle CREATE or UPDATE ---
            run_bq_merge(data)

    except json.JSONDecodeError as e:
        print(f"ERROR: Failed to decode JSON: {e}. Message data: {envelope['message']['data']}")
        # Don't retry, just acknowledge. A bad message shouldn't block the pipeline.
        return ("Message processing error", 200)
    except Exception as e:
        print(f"ERROR: Unexpected error processing message: {e}")
        return ("Message processing error", 200)

    # Acknowledge the message to Pub/Sub
    return ("", 204)

def run_bq_delete(data):
    """Builds and runs a DELETE query."""
    pk_id = data["id"]
    query = f"""
        DELETE FROM `{TABLE_ID}`
        WHERE id = {pk_id}
    """
    print(f"Running query: {query}")
    bq_client.query(query).result()
    print(f"Deleted row with ID {pk_id}")

def run_bq_merge(data):
    """Builds and runs a MERGE (UPSERT) query."""
    query = f"""
        MERGE `{TABLE_ID}` T
        USING (SELECT
            {data['id']} as id,
            '{data['first_name']}' as first_name,
            '{data['last_name']}' as last_name,
            '{data['email']}' as email
        ) S
        ON T.id = S.id
        WHEN MATCHED THEN
            UPDATE SET
                first_name = S.first_name,
                last_name = S.last_name,
                email = S.email
        WHEN NOT MATCHED THEN
            INSERT (id, first_name, last_name, email)
            VALUES (S.id, S.first_name, S.last_name, S.email)
    """
    print(f"Running query: {query}")
    bq_client.query(query).result()
    print(f"Merged row with ID {data['id']}")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
