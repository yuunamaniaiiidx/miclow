use crate::topic::Topic;

#[derive(Debug, Clone)]
pub enum SystemCommand {
    Pop(Topic),
    Peek(Topic),
    Latest(Topic),
    Result(Topic),
    PopAwait(Topic),
}

impl SystemCommand {
    pub fn parse(command_topic: &str, topic_data: &str) -> Option<Self> {
        let trimmed = topic_data.trim();
        if trimmed.is_empty() {
            return None;
        }
        let topic = Topic::from(trimmed);
        match command_topic {
            "system.pop" => Some(Self::Pop(topic)),
            "system.peek" => Some(Self::Peek(topic)),
            "system.latest" => Some(Self::Latest(topic)),
            "system.result" => Some(Self::Result(topic)),
            "system.pop_await" => Some(Self::PopAwait(topic)),
            _ => None,
        }
    }

    pub fn topic(&self) -> &Topic {
        match self {
            Self::Pop(topic)
            | Self::Peek(topic)
            | Self::Latest(topic)
            | Self::Result(topic)
            | Self::PopAwait(topic) => topic,
        }
    }
}
