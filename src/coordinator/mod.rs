use std::collections::HashMap;
use std::sync::Arc;

use crate::channels::UserLogSender;
use crate::config::SystemConfig;
use crate::subscription::{
    ConsumerStartContext, SubscriptionController, SubscriptionId, SubscriptionSpec,
};
use crate::topic::TopicSubscriptionRegistry;
use tokio_util::sync::CancellationToken;

pub struct CoordinatorManager {
    subscription_controller: Option<SubscriptionController>,
    topic_manager: TopicSubscriptionRegistry,
    subscriptions: HashMap<Arc<str>, SubscriptionId>,
}

impl CoordinatorManager {
    pub fn new(
        topic_manager: TopicSubscriptionRegistry,
        shutdown_token: CancellationToken,
    ) -> Self {
        Self {
            subscription_controller: Some(SubscriptionController::new(shutdown_token)),
            topic_manager,
            subscriptions: HashMap::new(),
        }
    }

    pub fn start_all(&mut self, config: &SystemConfig, userlog_sender: UserLogSender) {
        let Some(controller) = self.subscription_controller.as_mut() else {
            log::warn!("CoordinatorManager start_all called after shutdown");
            return;
        };

        let topic_manager = self.topic_manager.clone();

        for task in config.tasks.values() {
            let start_context = ConsumerStartContext {
                protocol_backend: task.protocol_backend.clone(),
                topic_manager: topic_manager.clone(),
                userlog_sender: userlog_sender.clone(),
                view_stdout: task.view_stdout,
                view_stderr: task.view_stderr,
            };

            let spec = SubscriptionSpec {
                task_name: task.name.clone(),
                desired_instances: task.lifecycle.desired_instances,
                start_context,
            };
            let subscription_id = controller.create_subscription(spec);
            log::info!(
                "Created Subscription {} for task '{}' (desired instances: {})",
                subscription_id,
                task.name.as_ref(),
                task.lifecycle.desired_instances
            );
            self.subscriptions
                .insert(task.name.clone(), subscription_id);
        }
    }

    pub async fn shutdown(mut self) {
        if let Some(controller) = self.subscription_controller.take() {
            controller.shutdown_all().await;
        }
    }
}
