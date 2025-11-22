import miclow

print("Sending message...")
miclow.send_message("mytopic", "Hello, World!")

print("Waiting for message...")

# The message never arrives because it was sent by the same task
print(miclow.wait_for_topic("mytopic"))

print("Done")
