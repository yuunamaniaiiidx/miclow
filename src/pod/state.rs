use crate::pod::PodId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PodState {
    Idle,
    Busy(PodId), // メッセージを送信した元のPodのIDを保存
}

impl Default for PodState {
    fn default() -> Self {
        Self::Idle
    }
}
