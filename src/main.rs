mod collector;
mod exchange;
mod kalshi_adapter;

use std::sync::Arc;
use tokio::sync::watch;

use collector::Config;
use engineering_challenge::util::AlignedSbeWriter;
use exchange::ExchangeAdapter;
use kalshi_adapter::KalshiAdapter;

const CHUNK_SIZE_64_MEBIBYTES: usize = 64 << 20;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    match dotenvy::dotenv() {
        Ok(_) => {}
        Err(dotenvy::Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }

    let config = Config::from_env()?;
    let adapter: Arc<dyn ExchangeAdapter> = Arc::new(KalshiAdapter::from_env()?);
    let writer = AlignedSbeWriter::create(&config.output_path, CHUNK_SIZE_64_MEBIBYTES)?;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let signal_shutdown = shutdown_tx.clone();
    let signal_task = tokio::spawn(async move {
        if let Err(error) = tokio::signal::ctrl_c().await {
            eprintln!("failed to listen for SIGINT: {error}");
        } else {
            eprintln!("SIGINT received; closing exchange connections and flushing the SBE file");
            let _ = signal_shutdown.send(true);
        }
    });

    let result = collector::run(config, adapter, writer, shutdown_tx, shutdown_rx).await;
    signal_task.abort();
    result
}
