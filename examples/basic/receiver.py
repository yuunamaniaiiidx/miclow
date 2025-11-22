import miclow
import time
import uuid

myid = uuid.uuid4()


TOPIC = "demo.topic"

print(f"[receiver {myid}] waiting for topic {TOPIC}")

message = miclow.receive(TOPIC)
print(f"[receiver {myid}] {message.data}")
time.sleep(0)
miclow.publish_response(TOPIC, f"Hello from receiver {myid}")
