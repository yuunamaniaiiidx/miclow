while True:
    import uuid
    print('"call":', "called from 1: " + str(uuid.uuid4()))
    data = ""
    while data == "":
        print('"system.result": call.result')
        data = "".join(input() for _ in range(int(input())))
    print(data)
