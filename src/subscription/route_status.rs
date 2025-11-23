use crate::messages::ExecutorOutputEvent;

pub enum RouteStatus {
    Delivered,
    Pending(ExecutorOutputEvent),
    Dropped,
}

