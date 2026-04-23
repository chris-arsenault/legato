//! Invalidation fan-out primitives for server-side metadata and cache refresh.

use std::collections::HashMap;

use legato_proto::{InvalidationEvent, InvalidationKind};

/// Subscription handle returned to connected clients.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InvalidationSubscription {
    /// Server-local subscriber identifier.
    pub subscriber_id: u64,
    /// Initial invalidations delivered immediately on subscribe/reconnect.
    pub initial_events: Vec<InvalidationEvent>,
}

/// In-memory invalidation publisher with per-subscriber fan-out queues.
#[derive(Debug)]
pub struct InvalidationHub {
    library_root: String,
    next_subscriber_id: u64,
    subscribers: HashMap<u64, Vec<InvalidationEvent>>,
}

impl InvalidationHub {
    /// Creates a new hub rooted at the canonical library path.
    #[must_use]
    pub fn new(library_root: impl Into<String>) -> Self {
        Self {
            library_root: library_root.into(),
            next_subscriber_id: 1,
            subscribers: HashMap::new(),
        }
    }

    /// Registers a subscriber and returns the initial reconnect-safe invalidation.
    pub fn subscribe(&mut self) -> InvalidationSubscription {
        let subscriber_id = self.next_subscriber_id;
        self.next_subscriber_id += 1;
        self.subscribers.insert(subscriber_id, Vec::new());

        InvalidationSubscription {
            subscriber_id,
            initial_events: vec![subtree_invalidation(&self.library_root, 0)],
        }
    }

    /// Removes a subscriber, ignoring unknown IDs.
    pub fn unsubscribe(&mut self, subscriber_id: u64) {
        self.subscribers.remove(&subscriber_id);
    }

    /// Removes every active subscriber.
    pub fn clear_subscribers(&mut self) {
        self.subscribers.clear();
    }

    /// Publishes an invalidation to every active subscriber.
    pub fn publish(&mut self, event: InvalidationEvent) {
        for queue in self.subscribers.values_mut() {
            queue.push(event.clone());
        }
    }

    /// Publishes a batch of invalidations preserving input order.
    pub fn publish_all<I>(&mut self, events: I)
    where
        I: IntoIterator<Item = InvalidationEvent>,
    {
        for event in events {
            self.publish(event);
        }
    }

    /// Drains the queued invalidations for one subscriber.
    pub fn drain(&mut self, subscriber_id: u64) -> Option<Vec<InvalidationEvent>> {
        self.subscribers.get_mut(&subscriber_id).map(std::mem::take)
    }

    /// Returns the number of active subscribers.
    #[must_use]
    pub fn subscriber_count(&self) -> usize {
        self.subscribers.len()
    }
}

/// Builds a subtree invalidation event for the supplied path.
#[must_use]
pub fn subtree_invalidation(path: &str, file_id: u64) -> InvalidationEvent {
    InvalidationEvent {
        kind: InvalidationKind::Subtree as i32,
        path: String::from(path),
        file_id,
    }
}

#[cfg(test)]
mod tests {
    use super::{InvalidationHub, subtree_invalidation};

    #[test]
    fn subscribe_emits_reconnect_safe_root_invalidation() {
        let mut hub = InvalidationHub::new("/");

        let subscription = hub.subscribe();

        assert_eq!(hub.subscriber_count(), 1);
        assert_eq!(subscription.subscriber_id, 1);
        assert_eq!(
            subscription.initial_events,
            vec![subtree_invalidation("/", 0)]
        );
    }

    #[test]
    fn published_invalidations_are_fanned_out_to_every_subscriber() {
        let mut hub = InvalidationHub::new("/");
        let first = hub.subscribe();
        let second = hub.subscribe();

        hub.publish_all([
            subtree_invalidation("/Kontakt", 12),
            subtree_invalidation("/Spitfire", 44),
        ]);

        assert_eq!(
            hub.drain(first.subscriber_id)
                .expect("first subscriber should exist"),
            vec![
                subtree_invalidation("/Kontakt", 12),
                subtree_invalidation("/Spitfire", 44),
            ]
        );
        assert_eq!(
            hub.drain(second.subscriber_id)
                .expect("second subscriber should exist"),
            vec![
                subtree_invalidation("/Kontakt", 12),
                subtree_invalidation("/Spitfire", 44),
            ]
        );
    }

    #[test]
    fn unsubscribe_stops_future_delivery() {
        let mut hub = InvalidationHub::new("/");
        let first = hub.subscribe();
        let second = hub.subscribe();
        hub.unsubscribe(first.subscriber_id);

        hub.publish(subtree_invalidation("/Kontakt", 12));

        assert_eq!(hub.subscriber_count(), 1);
        assert!(hub.drain(first.subscriber_id).is_none());
        assert_eq!(
            hub.drain(second.subscriber_id)
                .expect("second subscriber should exist"),
            vec![subtree_invalidation("/Kontakt", 12)]
        );
    }
}
