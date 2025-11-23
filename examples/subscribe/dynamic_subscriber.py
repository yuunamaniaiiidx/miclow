import miclow

for i in range(5):
    print('"system.subscribe-topic": ', "topic" + str(i))
    print('"system.subscribe-topic.result": "dummy"')
    message = (miclow.receive())
    print(message)

while True:
    print(miclow.receive())
