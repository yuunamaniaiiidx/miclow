import uuid

print('"call":', "called from 2: " + str(uuid.uuid4()))
data = ""
while data == "":
    print('"system.return": return.call')
    data = "".join(input() for _ in range(int(input())))
print(data)
