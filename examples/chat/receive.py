import miclow

while True:
    message = miclow.wait_for_topic("data")
    print(message)
