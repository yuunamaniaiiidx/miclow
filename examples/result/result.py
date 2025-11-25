data = ""
while data == "":
    print('"system.pop": call')
    data = "".join(input() for _ in range(int(input())))

print('"call.result"::')
print("result")
print(data)
print('::"call.result"')
