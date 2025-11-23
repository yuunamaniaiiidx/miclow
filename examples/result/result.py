while True:
    data = ""
    while data == "":
        print('"system.pull": call')
        data = "".join(input() for _ in range(int(input())))

    print('"call.result"::')
    print("result")
    print(data)
    print('::"call.result"')
