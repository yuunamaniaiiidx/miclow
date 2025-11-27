import time
import json

print('"read_file": config.toml')
print('"system.return_await": read_file')
data = "".join([input() for _ in range(int(input()))])
print(json.loads(data))
time.sleep(3)
