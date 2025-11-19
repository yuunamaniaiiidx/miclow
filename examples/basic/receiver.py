import miclow

TOPIC = "demo.topic"

message = miclow.wait_for_topic(TOPIC)
print(f"[receiver] {message.data}")
