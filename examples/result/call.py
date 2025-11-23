import uuid
import time

print('"call":', uuid.uuid4())
data = ""
while data == "":
    print('"system.pull": call.result')
    data = "".join(input() for _ in range(int(input())))
print(data)
