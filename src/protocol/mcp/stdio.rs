use anyhow::{Context, Result};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use super::jsonrpc::JsonRpcMessage;

pub struct StdioTransport {
    stdin: ChildStdin,
    stdout_handle: JoinHandle<()>,
    message_tx: mpsc::UnboundedSender<JsonRpcMessage>,
}

impl StdioTransport {
    pub async fn new(mut child: Child) -> Result<(Self, mpsc::UnboundedReceiver<JsonRpcMessage>)> {
        let stdin = child.stdin.take()
            .ok_or_else(|| anyhow::anyhow!("Failed to take stdin from child process"))?;
        let stdout = child.stdout.take()
            .ok_or_else(|| anyhow::anyhow!("Failed to take stdout from child process"))?;

        let (message_tx, message_rx) = mpsc::unbounded_channel();

        let message_tx_clone = message_tx.clone();
        let stdout_handle = tokio::spawn(async move {
            let mut reader = BufReader::new(stdout);
            let mut line = String::new();

            loop {
                line.clear();
                match reader.read_line(&mut line).await {
                    Ok(0) => {
                        // EOF
                        break;
                    }
                    Ok(_) => {
                        let trimmed = line.trim();
                        if trimmed.is_empty() {
                            continue;
                        }

                        match serde_json::from_str::<JsonRpcMessage>(trimmed) {
                            Ok(msg) => {
                                if message_tx_clone.send(msg).is_err() {
                                    break;
                                }
                            }
                            Err(e) => {
                                log::warn!("Failed to parse JSON-RPC message: {} - Line: {}", e, trimmed);
                            }
                        }
                    }
                    Err(e) => {
                        log::error!("Error reading from stdout: {}", e);
                        break;
                    }
                }
            }
        });

        Ok((
            Self {
                stdin,
                stdout_handle,
                message_tx,
            },
            message_rx,
        ))
    }

    pub async fn send(&mut self, message: JsonRpcMessage) -> Result<()> {
        let json = serde_json::to_string(&message)
            .context("Failed to serialize JSON-RPC message")?;
        
        self.stdin.write_all(json.as_bytes()).await
            .context("Failed to write to stdin")?;
        self.stdin.write_all(b"\n").await
            .context("Failed to write newline to stdin")?;
        self.stdin.flush().await
            .context("Failed to flush stdin")?;

        Ok(())
    }

    pub fn message_sender(&self) -> mpsc::UnboundedSender<JsonRpcMessage> {
        self.message_tx.clone()
    }

    pub async fn shutdown(self) {
        drop(self.stdin);
        let _ = self.stdout_handle.await;
    }
}

