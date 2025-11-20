import miclow
import time
import uuid

myid = uuid.uuid4()


TOPIC = "demo.topic"


message = miclow.wait_for_topic(TOPIC)
print(f"[receiver {myid}] {message.data}")
time.sleep(1)
