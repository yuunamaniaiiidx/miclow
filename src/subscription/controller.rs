use std::collections::HashMap;
use crate::shutdown_registry::ShutdownRegistry;
use crate::subscription::context::SubscriptionSpec;
use crate::subscription::SubscriptionId;
use crate::subscription::worker::SubscriptionWorker;
use tokio_util::sync::CancellationToken;

pub struct SubscriptionController {
    shutdown_token: CancellationToken,
    shutdown_registry: ShutdownRegistry,
    subscriptions: HashMap<SubscriptionId, String>, // subscription_id -> task_name
}

impl SubscriptionController {
    pub fn new(shutdown_token: CancellationToken) -> Self {
        Self {
            shutdown_token: shutdown_token.clone(),
            shutdown_registry: ShutdownRegistry::with_shutdown_token(shutdown_token),
            subscriptions: HashMap::new(),
        }
    }

    pub fn create_subscription(&mut self, spec: SubscriptionSpec) -> SubscriptionId {
        let desired_instances = spec.desired_instances.max(1);
        let subscription_id = SubscriptionId::new();
        let subscription_token = self.shutdown_token.child_token();

        let task_handle = self.shutdown_registry.create_task_handle();
        let worker = SubscriptionWorker::new(
            subscription_id.clone(),
            spec.task_name.clone(),
            desired_instances,
            spec.start_context,
            subscription_token.clone(),
            task_handle,
        );

        let subscription_task_handle = self.shutdown_registry.create_task_handle();
        let task_name = format!("subscription_{}", subscription_id);
        
        let _handle = subscription_task_handle.run(task_name.clone(), async move {
            worker.run().await;
        }).expect("Failed to send task info");

        self.subscriptions.insert(
            subscription_id.clone(),
            spec.task_name.to_string(),
        );

        subscription_id
    }

    pub async fn shutdown_all(mut self) {
        // ShutdownRegistryがすべてのタスクを管理しているので、shutdown_allを呼ぶだけ
        self.shutdown_registry.shutdown_all(std::time::Duration::from_secs(10)).await;
    }
}
