import miclow
print("Sending message...")
miclow.publish("mytopic", "Hello, World!")

print("Waiting for message...")

print(miclow.receive("mytopic"))

print("Done")
