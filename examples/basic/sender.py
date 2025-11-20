import miclow
import time

TOPIC = "demo.topic"
TOTAL_MESSAGES = 5
DELAY_SEC = 1


for idx in range(1, TOTAL_MESSAGES + 1):
    payload = f"[{idx}/{TOTAL_MESSAGES}] hello from sender"
    miclow.send_message(TOPIC, payload)
    print(f"Sent to {TOPIC}: {payload}")
    time.sleep(DELAY_SEC)
