/// すべてのレスポンス topic が従うサフィックス。
const RESULT_TOPIC_SUFFIX: &str = ".result";

/// Topic型 - トピック名を型安全に管理し、レスポンストピックを自動生成できる
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Topic {
    name: String,
}

impl Topic {
    /// 新しいTopicを作成
    pub fn new(name: String) -> Self {
        Self { name }
    }

    /// トピック名を文字列スライスとして取得
    pub fn as_str(&self) -> &str {
        &self.name
    }

    /// トピック名を所有するStringとして取得
    pub fn into_string(self) -> String {
        self.name
    }

    /// このトピックのレスポンストピックを生成
    /// 例: "my.topic" -> "my.topic.result"
    pub fn result(&self) -> Topic {
        Topic::new(format!("{}{}", self.name, RESULT_TOPIC_SUFFIX))
    }

    /// このトピックがレスポンストピックかどうかを判定
    pub fn is_result(&self) -> bool {
        self.name.ends_with(RESULT_TOPIC_SUFFIX)
    }

    /// レスポンストピックから元のトピックを取得
    /// レスポンストピックでない場合はNoneを返す
    pub fn original(&self) -> Option<Topic> {
        if self.is_result() {
            let original_name = self.name.strip_suffix(RESULT_TOPIC_SUFFIX)?;
            Some(Topic::new(original_name.to_string()))
        } else {
            None
        }
    }
}

impl From<String> for Topic {
    fn from(s: String) -> Self {
        Self::new(s)
    }
}

impl From<&str> for Topic {
    fn from(s: &str) -> Self {
        Self::new(s.to_string())
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

