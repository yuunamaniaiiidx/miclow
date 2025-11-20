import miclow
import time
import uuid

myid = uuid.uuid4()


TOPIC = "demo.topic"

while True:
    message = miclow.wait_for_topic(TOPIC)
    print(f"[receiver {myid}] {message.data}")
    time.sleep(2)
    miclow.send_response(TOPIC, f"Hello from receiver {myid}")
