import miclow

for i in range(5):
    print(f"Dynamic Subscriber: Subscribing to 'topic{i}'")
    response1 = miclow.subscribe(f"topic{i}")
    print(response1)

print(miclow.receive())
