use std::collections::HashMap;

use crate::channels::UserLogSender;
use crate::config::SystemConfig;
use crate::replicaset::{PodStartContext, ReplicaSetController, ReplicaSetId, ReplicaSetSpec};
use crate::topic::TopicSubscriptionRegistry;
use tokio_util::sync::CancellationToken;

pub struct DeploymentManager {
    replicaset_controller: Option<ReplicaSetController>,
    topic_manager: TopicSubscriptionRegistry,
    deployments: HashMap<String, ReplicaSetId>,
}

impl DeploymentManager {
    pub fn new(
        topic_manager: TopicSubscriptionRegistry,
        shutdown_token: CancellationToken,
    ) -> Self {
        Self {
            replicaset_controller: Some(ReplicaSetController::new(shutdown_token)),
            topic_manager,
            deployments: HashMap::new(),
        }
    }

    pub fn start_all(&mut self, config: &SystemConfig, userlog_sender: UserLogSender) {
        let Some(controller) = self.replicaset_controller.as_mut() else {
            log::warn!("DeploymentManager start_all called after shutdown");
            return;
        };

        let topic_manager = self.topic_manager.clone();

        for task in config.tasks.values() {
            let start_context = PodStartContext {
                protocol_backend: task.protocol_backend.clone(),
                topic_manager: topic_manager.clone(),
                userlog_sender: userlog_sender.clone(),
                subscribe_topics: task.subscribe_topics.clone(),
                private_response_topics: task.private_response_topics.clone(),
                view_stdout: task.view_stdout,
                view_stderr: task.view_stderr,
            };

            let spec = ReplicaSetSpec {
                task_name: task.name.clone(),
                desired_instances: task.lifecycle.desired_instances,
                start_context,
            };
            let replicaset_id = controller.create_replicaset(spec);
            log::info!(
                "Created ReplicaSet {} for task '{}' (desired instances: {})",
                replicaset_id,
                task.name,
                task.lifecycle.desired_instances
            );
            self.deployments.insert(task.name.clone(), replicaset_id);
        }
    }

    pub async fn shutdown(mut self) {
        if let Some(controller) = self.replicaset_controller.take() {
            controller.shutdown_all().await;
        }
    }
}
