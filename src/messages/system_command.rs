use crate::topic::Topic;

#[derive(Debug, Clone)]
pub enum SystemCommand {
    Pop(Topic),
    Peek(Topic),
    Latest(Topic),
    Return(Topic),
    PopAwait(Topic),
    ReturnAwait(Topic),
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
            "system.return" => Some(Self::Return(topic)),
            "system.pop_await" => Some(Self::PopAwait(topic)),
            "system.return_await" => Some(Self::ReturnAwait(topic)),
            _ => None,
        }
    }

    pub fn topic(&self) -> &Topic {
        match self {
            Self::Pop(topic)
            | Self::Peek(topic)
            | Self::Latest(topic)
            | Self::Return(topic)
            | Self::PopAwait(topic)
            | Self::ReturnAwait(topic) => topic,
        }
    }
}
