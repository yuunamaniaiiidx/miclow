use uuid::Uuid;

/// ReplicaSet を一意に識別するためのID
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ReplicaSetId(Uuid);

impl ReplicaSetId {
    /// 新しい ReplicaSetId を生成
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }
}

impl std::fmt::Display for ReplicaSetId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

