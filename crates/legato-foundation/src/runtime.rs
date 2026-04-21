//! Reusable startup and shutdown primitives.

use tokio_util::sync::CancellationToken;

/// Owner-side handle for requesting process shutdown.
#[derive(Clone, Debug, Default)]
pub struct ShutdownController {
    token: CancellationToken,
}

impl ShutdownController {
    /// Creates a new controller and backing cancellation token.
    #[must_use]
    pub fn new() -> Self {
        Self {
            token: CancellationToken::new(),
        }
    }

    /// Returns a cloneable token that tasks can observe.
    #[must_use]
    pub fn token(&self) -> ShutdownToken {
        ShutdownToken {
            token: self.token.clone(),
        }
    }

    /// Requests shutdown for all observers of the associated token.
    pub fn shutdown(&self) {
        self.token.cancel();
    }
}

/// Cloneable shutdown token shared with background tasks.
#[derive(Clone, Debug)]
pub struct ShutdownToken {
    token: CancellationToken,
}

impl ShutdownToken {
    /// Returns whether cancellation has been requested.
    #[must_use]
    pub fn is_shutdown_requested(&self) -> bool {
        self.token.is_cancelled()
    }
}

#[cfg(test)]
mod tests {
    use super::ShutdownController;

    #[test]
    fn shutdown_controller_cancels_shared_tokens() {
        let controller = ShutdownController::new();
        let token = controller.token();

        assert!(!token.is_shutdown_requested());
        controller.shutdown();
        assert!(token.is_shutdown_requested());
    }
}
