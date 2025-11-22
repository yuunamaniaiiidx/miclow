use std::collections::HashMap;
use crate::replicaset::context::ReplicaSetSpec;
use crate::replicaset::ReplicaSetId;
use crate::replicaset::worker::ReplicaSetWorker;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub struct ReplicaSetController {
    shutdown_token: CancellationToken,
    replica_sets: HashMap<ReplicaSetId, ReplicaSetHandle>,
}

struct ReplicaSetHandle {
    join_handle: JoinHandle<()>,
    shutdown_token: CancellationToken,
}

impl ReplicaSetController {
    pub fn new(shutdown_token: CancellationToken) -> Self {
        Self {
            shutdown_token,
            replica_sets: HashMap::new(),
        }
    }

    pub fn create_replicaset(&mut self, spec: ReplicaSetSpec) -> ReplicaSetId {
        let desired_instances = spec.desired_instances.max(1);
        let replicaset_id = ReplicaSetId::new();
        let replica_token = self.shutdown_token.child_token();

        let worker = ReplicaSetWorker::new(
            replicaset_id.clone(),
            spec.task_name,
            desired_instances,
            spec.start_context,
            replica_token.clone(),
        );

        let join_handle = tokio::spawn(worker.run());

        self.replica_sets.insert(
            replicaset_id.clone(),
            ReplicaSetHandle {
                join_handle,
                shutdown_token: replica_token,
            },
        );

        replicaset_id
    }

    pub async fn shutdown_all(mut self) {
        for (_id, handle) in self.replica_sets.drain() {
            handle.shutdown_token.cancel();
            let _ = handle.join_handle.await;
        }
    }
}
