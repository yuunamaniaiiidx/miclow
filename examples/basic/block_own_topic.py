import miclow
print("Sending message...")
miclow.publish("mytopic", "Hello, World!")

print("Waiting for message...")

# The message never arrives because it was sent by the same task
print(miclow.receive("mytopic"))

print("Done")
