import miclow
import time

TOPIC = "demo.topic"
TOTAL_MESSAGES = 5

for idx in range(1, TOTAL_MESSAGES + 1):
    payload = f"[{idx}/{TOTAL_MESSAGES}] hello from sender"
    miclow.send_message(TOPIC, payload)
    print(f"Sent to {TOPIC}: {payload}")
    response = miclow.wait_for_response(TOPIC)
    print(f"Received response: {response.data}")
    time.sleep(1)
