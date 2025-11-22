import miclow
import time

while True:
    print("pull message...")

    # print(miclow.pull("apply"))
    print('"system.pull": apply')
    print(input())
    for _ in range(int(input())):
        print(input())
    time.sleep(1)
    print('"system.idle": ')
