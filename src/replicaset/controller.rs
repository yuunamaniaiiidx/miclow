use std::collections::HashMap;
use crate::subscription::context::SubscriptionSpec;
use crate::subscription::SubscriptionId;
use crate::subscription::worker::SubscriptionWorker;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub struct SubscriptionController {
    shutdown_token: CancellationToken,
    subscriptions: HashMap<SubscriptionId, SubscriptionHandle>,
}

struct SubscriptionHandle {
    join_handle: JoinHandle<()>,
    shutdown_token: CancellationToken,
}

impl SubscriptionController {
    pub fn new(shutdown_token: CancellationToken) -> Self {
        Self {
            shutdown_token,
            subscriptions: HashMap::new(),
        }
    }

    pub fn create_subscription(&mut self, spec: SubscriptionSpec) -> SubscriptionId {
        let desired_instances = spec.desired_instances.max(1);
        let subscription_id = SubscriptionId::new();
        let subscription_token = self.shutdown_token.child_token();

        let worker = SubscriptionWorker::new(
            subscription_id.clone(),
            spec.task_name,
            desired_instances,
            spec.start_context,
            subscription_token.clone(),
        );

        let join_handle = tokio::spawn(worker.run());

        self.subscriptions.insert(
            subscription_id.clone(),
            SubscriptionHandle {
                join_handle,
                shutdown_token: subscription_token,
            },
        );

        subscription_id
    }

    pub async fn shutdown_all(mut self) {
        for (_id, handle) in self.subscriptions.drain() {
            handle.shutdown_token.cancel();
            let _ = handle.join_handle.await;
        }
    }
}
