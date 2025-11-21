import miclow
import time
import uuid

myid = uuid.uuid4()


TOPIC = "demo.topic"

print(f"[receiver {myid}] waiting for topic {TOPIC}")

message = miclow.wait_for_topic(TOPIC)
print(f"[receiver {myid}] {message.data}")
time.sleep(0)
miclow.send_response(TOPIC, f"Hello from receiver {myid}")
