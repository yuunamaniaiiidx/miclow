import time

while True:
    print("\"system.start-task\": ", "json-generator")
    print("\"system.stdout\": ", input())
    for _ in range(int(input())):
        print("\"system.stdout\": ", input())

    time.sleep(1)