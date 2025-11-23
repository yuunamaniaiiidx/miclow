import miclow
import time

for i in range(5):
    message = f"Message {i} from publisher"
    miclow.publish(f"topic{i}", message)
    print(f"Publisher: Sent message {i}: {message}")
    time.sleep(1)
