import miclow

TOPIC = "demo.topic"


def main() -> None:
    print(f"Waiting for messages on '{TOPIC}' ...")
    while True:
        message = miclow.wait_for_topic(TOPIC)
        if message is None:
            continue

        print(f"[receiver] {message.data}")


if __name__ == "__main__":
    main()

