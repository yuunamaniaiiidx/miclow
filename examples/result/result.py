data = ""
while data == "":
    print('"system.pull": call')
    data = "".join(input() for _ in range(int(input())))
print('"call.result"::')
print(data)
print('::"call.result"')
