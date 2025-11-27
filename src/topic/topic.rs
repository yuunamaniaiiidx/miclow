use std::sync::Arc;

const RESULT_TOPIC_PREFIX: &str = "return.";

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Topic {
    Normal(Arc<str>),
    Result(Arc<str>),
}

impl Topic {
    pub fn new(name: impl Into<Arc<str>>) -> Self {
        let name = name.into();
        if name.starts_with(RESULT_TOPIC_PREFIX) {
            let original_name = name.strip_prefix(RESULT_TOPIC_PREFIX)
                .unwrap_or(&name);
            Self::Result(Arc::from(original_name))
        } else {
            Self::Normal(name)
        }
    }

    pub fn name(&self) -> &str {
        match self {
            Topic::Normal(name) => name,
            Topic::Result(name) => name,
        }
    }

    pub fn as_str(&self) -> &str {
        self.name()
    }

    pub fn to_return(&self) -> Topic {
        match self {
            Topic::Normal(name) => Topic::Result(name.clone()),
            Topic::Result(_) => self.clone(),
        }
    }

    pub fn to_normal(&self) -> Topic {
        match self {
            Topic::Normal(_) => self.clone(),
            Topic::Result(name) => Topic::Normal(name.clone()),
        }
    }
}

impl From<String> for Topic {
    fn from(s: String) -> Self {
        Self::new(Arc::from(s))
    }
}

impl From<&str> for Topic {
    fn from(s: &str) -> Self {
        Self::new(Arc::from(s))
    }
}

impl From<Topic> for String {
    fn from(topic: Topic) -> Self {
        topic.to_string()
    }
}

impl AsRef<str> for Topic {
    fn as_ref(&self) -> &str {
        self.name()
    }
}

impl std::fmt::Display for Topic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Topic::Normal(name) => write!(f, "{}", name),
            Topic::Result(name) => write!(f, "{}{}", RESULT_TOPIC_PREFIX, name),
        }
    }
}
