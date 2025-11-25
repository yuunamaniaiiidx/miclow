use std::sync::Arc;

const RESULT_TOPIC_SUFFIX: &str = ".result";

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Topic {
    name: Arc<str>,
}

impl Topic {
    pub fn new(name: impl Into<Arc<str>>) -> Self {
        Self { name: name.into() }
    }

    pub fn as_str(&self) -> &str {
        &self.name
    }

    pub fn into_string(self) -> String {
        self.name.to_string()
    }

    pub fn result(&self) -> Topic {
        Topic::new(format!("{}{}", self.name, RESULT_TOPIC_SUFFIX))
    }

    pub fn is_result(&self) -> bool {
        self.name.ends_with(RESULT_TOPIC_SUFFIX)
    }

    pub fn original(&self) -> Option<Topic> {
        if self.is_result() {
            let original_name = self.name.strip_suffix(RESULT_TOPIC_SUFFIX)?;
            Some(Topic::new(Arc::from(original_name)))
        } else {
            None
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
        topic.into_string()
    }
}

impl AsRef<str> for Topic {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl std::fmt::Display for Topic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}
