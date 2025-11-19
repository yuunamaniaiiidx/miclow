# Basic Sender/Receiver Example

This example demonstrates the simplest round-trip setup under the new
always-on worker model.

## Tasks
* `basic_sender`: publishes 5 messages to `demo.topic`
* `basic_receiver`: subscribes to `demo.topic` and prints every payload

## Usage

1. From the `miclow` workspace root, run:

   ```bash
   cargo run -- --config ./examples/basic/config.toml
   ```

2. You should see the receiver logging the five payloads:

   ```
   Waiting for messages on 'demo.topic' ...
   [receiver] [1/5] hello from sender
   ...
   ```

Feel free to duplicate this config and change `demo.topic` to try out other
scenarios.

