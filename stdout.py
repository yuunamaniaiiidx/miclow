cnt = 0
while True:
    input()
    for _ in range(int(input())):
        s = input()
        print("\"system.stdout\": ", s)
    cnt += 1
    if cnt > 3:
        print("\"system.killserver\": ")
        break