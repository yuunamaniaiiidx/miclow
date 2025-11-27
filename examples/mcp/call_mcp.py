import time
import json

print('"read_file": config.toml')
print('"system.return_await": read_file')
DATA = "".join([input() for _ in range(int(input()))])
print(json.loads(DATA)["result"]["content"][0]["text"])
time.sleep(3)
