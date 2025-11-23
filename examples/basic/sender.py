import miclow
import time

TOPIC = "demo.topic"
TOTAL_MESSAGES = 1
for idx in range(1, TOTAL_MESSAGES + 1):
    payload = f"[{idx}/{TOTAL_MESSAGES}] hello from sender"
    miclow.publish(TOPIC, payload)
    print(f"Sent to {TOPIC}: {payload}")
    print(miclow.receive_response(TOPIC).data)
