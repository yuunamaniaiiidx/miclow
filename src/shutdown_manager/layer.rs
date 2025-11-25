#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ShutdownLayer {
    Logging,
    ControlPlane,
    UserFacing,
}

impl ShutdownLayer {
    pub fn priority(&self) -> u16 {
        match self {
            ShutdownLayer::Logging => 0,
            ShutdownLayer::ControlPlane => 10,
            ShutdownLayer::UserFacing => 20,
        }
    }

    pub fn label(&self) -> &'static str {
        match self {
            ShutdownLayer::Logging => "logging",
            ShutdownLayer::ControlPlane => "control-plane",
            ShutdownLayer::UserFacing => "user-facing",
        }
    }
}

impl PartialOrd for ShutdownLayer {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ShutdownLayer {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.priority().cmp(&other.priority())
    }
}
