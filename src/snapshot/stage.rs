//! Opt-in debug wall-time traces; no entered span survives an async await.

use std::{future::Future, time::Instant};

use tracing::{Instrument, Span};

struct Stage {
    span: Span,
    started: Option<Instant>,
    completed: bool,
}

impl Stage {
    fn new(stage: &'static str) -> Self {
        let span = tracing::debug_span!(
            target: "scorpiofs::workspace::performance",
            "workspace_stage",
            stage
        );
        let started = (!span.is_disabled()).then(Instant::now);
        Self {
            span,
            started,
            completed: false,
        }
    }
}

impl Drop for Stage {
    fn drop(&mut self) {
        if let Some(started) = self.started {
            tracing::debug!(
                target: "scorpiofs::workspace::performance",
                parent: &self.span,
                elapsed_us = started.elapsed().as_micros().min(u64::MAX as u128) as u64,
                completed = self.completed,
                "workspace stage wall time"
            );
        }
    }
}

/// `completed` means the future returned (including an error), not success.
/// A dropped pending future emits `completed=false`; elapsed time includes
/// awaits and scheduling. The subscriber controls whether timing is enabled.
pub(crate) async fn trace_async<T>(stage: &'static str, future: impl Future<Output = T>) -> T {
    let mut timer = Stage::new(stage);
    let output = future.instrument(timer.span.clone()).await;
    timer.completed = true;
    output
}

pub(crate) fn trace_sync<T>(stage: &'static str, operation: impl FnOnce() -> T) -> T {
    let mut timer = Stage::new(stage);
    let output = timer.span.in_scope(operation);
    timer.completed = true;
    output
}

/// Start timing before the task is queued and carry only the dispatcher/span
/// into its synchronous worker. No thread-local guard crosses an await.
pub(crate) async fn trace_blocking<T: Send + 'static>(
    stage: &'static str,
    operation: impl FnOnce() -> T + Send + 'static,
) -> Result<T, tokio::task::JoinError> {
    trace_async(stage, async {
        let parent = Span::current();
        let dispatcher = tracing::dispatcher::get_default(|current| current.clone());
        tokio::task::spawn_blocking(move || {
            tracing::dispatcher::with_default(&dispatcher, || parent.in_scope(operation))
        })
        .await
    })
    .await
}

#[cfg(test)]
mod tests {
    use std::{
        io::{self, Write},
        sync::{Arc, Mutex},
    };

    use super::*;

    #[derive(Clone)]
    struct Output(Arc<Mutex<Vec<u8>>>);
    impl Write for Output {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn returned_and_cancelled_futures_emit_separate_wall_times_without_entered_spans() {
        let output = Output(Arc::new(Mutex::new(vec![])));
        let sink = output.clone();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_max_level(tracing::Level::DEBUG)
            .with_writer(move || sink.clone())
            .finish();
        let _subscriber = tracing::subscriber::set_default(subscriber);
        assert_eq!(trace_async("returned", async { 7 }).await, 7);
        assert!(trace_blocking("blocking", || {
            tracing::enabled!(target: "scorpiofs::workspace::performance", tracing::Level::DEBUG)
        })
        .await
        .unwrap());
        {
            let future = trace_async("cancelled", std::future::pending::<()>());
            tokio::pin!(future);
            assert!(futures::poll!(future.as_mut()).is_pending());
            assert!(Span::current().id().is_none(), "pending poll leaked a span");
        }
        let log = String::from_utf8(output.0.lock().unwrap().clone()).unwrap();
        let returned = log.lines().find(|line| line.contains("returned")).unwrap();
        let cancelled = log.lines().find(|line| line.contains("cancelled")).unwrap();
        assert!(returned.contains("completed=true"));
        assert!(cancelled.contains("completed=false"));
        assert!(returned.contains("elapsed_us=") && cancelled.contains("elapsed_us="));
        assert!(log
            .lines()
            .any(|line| line.contains("blocking") && line.contains("completed=true")));
        assert!(Span::current().id().is_none());
    }

    #[test]
    fn disabled_tracing_does_not_start_a_clock_or_skip_the_operation() {
        tracing::subscriber::with_default(tracing::subscriber::NoSubscriber::default(), || {
            assert!(Stage::new("disabled").started.is_none());
            assert_eq!(trace_sync("disabled", || 9), 9);
        });
    }
}
