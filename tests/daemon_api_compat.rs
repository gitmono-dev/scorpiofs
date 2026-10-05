//! The old explicit dictionary daemon entry remains callable downstream.

use std::sync::Arc;

use scorpiofs::{daemon::daemon_main, fuse::MegaFuse, manager::ScorpioManager};
use tokio::{net::TcpListener, sync::oneshot};

async fn downstream(
    fuse: Arc<MegaFuse>,
    manager: ScorpioManager,
    shutdown: oneshot::Receiver<()>,
    listener: TcpListener,
) -> std::io::Result<()> {
    daemon_main(fuse, manager, shutdown, listener).await
}

#[test]
fn original_four_argument_daemon_entry_compiles() {
    let _consumer = downstream;
}
