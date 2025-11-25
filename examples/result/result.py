data = ""
while data == "":
    print('"system.pop": call')
    data = "".join(input() for _ in range(int(input())))

print('"return.call"::')
print("result")
print(data)
print('::"return.call"')
