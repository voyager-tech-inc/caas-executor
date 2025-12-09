// SPDX-FileCopyrightText: 2025 Voyager Technologies, Inc.
//
// SPDX-License-Identifier: MIT

use anyhow::{anyhow, Result};
use std::sync::{Arc, Condvar, Mutex};
use tokio::runtime::Runtime;
use tokio::sync::mpsc;

type MsgSender = mpsc::Sender<CounterId>;
type MsgReceiver = mpsc::Receiver<CounterId>;
type ShutdownFlag = Arc<(Mutex<bool>, Condvar)>;

/// The Generic PrometheusPushExportor
pub trait GenericPrometheusPushExportor: Send + Sync {
    fn increment(&self, _: CounterId) -> Result<()>;
    fn shutdown(&self);
}

#[derive(Debug)]
///
/// Prometheus push exporter that uses Rust's <https://docs.rs/reqwest/latest/reqwest/>
/// Http client API to push to updates to prometheus push gateway.
pub struct PushPromExporter {
    /// The tokio Runtime.
    runtime: Runtime,

    /// Queue sender side handle
    tx_que: MsgSender,

    // Shutdown flag to notify foreground thread that all messages have been read from
    // tx_que and pushed to Prometheus gateway.
    shutdown: ShutdownFlag,
}

#[derive(Debug)]
/// Selects the counter to increment
pub enum CounterId {
    /// Increments the CompressedFileCount counter by the value specified, the second
    /// argument is the value for the status label.  Typically Fail or Ok.
    CompressedFileCount(u32, String),

    /// Increments the DecompressedFileCount counter by the value specified, the second
    /// argument is the value for the status label.  Typically Fail or Ok.
    DecompressedFileCount(u32, String),

    /// Special message to notify reader thread to exit after processing all messages
    /// still pending on the queue
    Shutdown,
}

#[rustfmt::skip]
impl PushPromExporter
{
    /// Class constructor
    /// Note: grafana label is generated here for grafana integration workaround
    /// # Arguments
    /// * threads           - Number of tokio threads in the tko runtime
    /// * queue_depth       - Depth of queue, sender blocks if full
    /// * url               - Prometheus Push Gateway URL
    /// * data_type         - Compressed file data format. e.g.: Gzip
    /// * file_format       - Uncompressed file format: e.g.: video/mp4.
    /// * graf_label_prefix - Grafana graph label preix
    /// * config_hash       - Configuration hash that represents the CLI for the container
    fn new(
        threads:            usize,
        queue_depth:        usize,
        url:               &str,
        data_type:         &str,
        file_format:       &str,
        graf_label_prefix: &str,
        config_hash:       &str,
    ) -> Result<Self> {
        // Create a tokio runtime scheduler
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(threads)
            .enable_all()
            .build()?;

        // Create a new channel and capture send/recv ends
        let (tx_que, rx_que) = tokio::sync::mpsc::channel(queue_depth);

        // Spawn a tokio thread to read messages from recv end of the queue and send them
        // via HTTP to push gateway
        let url = url.to_string();

        // Make owned copies of these that they can be 'moved' into the spawned thread.
        // TODO: generate label here
        let data_type = data_type.to_owned();
        let file_format = file_format.to_owned();
        let config_hash = config_hash.to_owned();

        // generate grafana label
        // gets last 4 characters of a string and recollects them into a string.
        let hash_end: String = config_hash
            .to_string()
            .chars()
            .rev()
            .take(4)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect();

        let mut graf_label = format!("{graf_label_prefix}-{hash_end}");

        let container_name_env = crate::util::get_container_name_env();

        match container_name_env {
            Some(s) => { 
                log::info!("The container name is: {}", s);
                graf_label = s;
            }
            None => log::info!("The container name is None"),
        }

        let shutdown = Arc::new((Mutex::new(false),Condvar::new()));
        let shutdown2 = shutdown.clone();

        // Spawn a background tokio thread that will continually read from the message
        // queue and push the Prometheus events to the prom gravel gateway

        let handle = runtime.handle().spawn(async move {
            // Takes ownership of all input parameters
            Self::async_consumer(
                shutdown2,  &data_type, &file_format, &config_hash, &graf_label, url, rx_que).await;
        });

        // Clippy says you should either call await or drop the task.  We can't call await
        // on it since this (caller) is not an tokio thread,
        std::mem::drop(handle);

        // Create and return class instance
        Ok(Self { runtime, shutdown, tx_que })
    }

    async fn async_consumer(
        shutdown: ShutdownFlag,
        data_type: &str,
        file_format: &str,
        config_hash: &str,
        grafana_label: &str,
        url: String,
        mut rx_que: MsgReceiver)
    {
        let fname = "PushPromExporter::PushThread";
        let op_comp = "caas_total_compressed_files";
        let op_decomp = "caas_total_decompressed_files";

        log::info!("{fname} ...");

        while let Some(msg) = rx_que.recv().await
        {
            log::trace!("{fname}: processing:{:?}", msg);

            // Get message payload
            let (op, count, status) = match msg
            {
                CounterId::CompressedFileCount(count,status) => (op_comp, count, status),
                CounterId::DecompressedFileCount(count,status) => (op_decomp, count, status),
                CounterId::Shutdown => 
                {
                    log::debug!("{fname}: Close write end of que and process pending messages...");
                    rx_que.close();
                    continue;
                }
            };
            let body = format!(
                r#"{op}{{graf_label="{grafana_label}",config_hash="{config_hash}",file_format="{file_format}",data_type="{data_type}",status="{status}"}} {count}
"#);
            log::debug!("{fname}: sending : {body}");
            let client = reqwest::Client::new();
            let resp = match client.post(&url).body(body).send().await
            {
                Ok(resp) => resp,
                Err(err) =>
                {
                    log::warn!("{fname} ... HTTP send failed:{err}");
                    continue;
                }
            };

            // Get HTTP response from prometheus exporter to check for errors
            log::trace!("{fname} ... calling resp.text()...");
            match resp.text().await
            {
                Ok(resp) =>
                {
                    // If response is not zero length, it's an error
                    if !resp.is_empty()
                    {
                        log::error!("{fname}: Got {}", resp.replace("\\n", "\n"));
                    }
                    continue;
                },
                Err(err) => log::error!("{fname}: Failed to get a response:{err}")
            }
        }
        log::debug!("{fname}: Setting shutdown flag...");
        crate::set_flag!(shutdown);

        #[cfg(test)]
        { println!("{fname}: ... done"); }
        log::info!("{fname}: ... done");
    }
}

impl GenericPrometheusPushExportor for PushPromExporter {
    /// Mostly Non blocking call to queue up a message to the push gateway, message will
    /// be sent in the background. See CounterId class at end of this file. It is mostly
    /// non-blocking because it blocks when the queue is full and the queue is bounded to
    /// avoid consuming all memory if the there is a network backup or a design error.
    //
    fn increment(&self, value: CounterId) -> Result<()> {
        // Sender.send() method may be called by a non-tokio task. It doesn't block if
        // there is still capacity in the queue to accept the message. Otherwise it blocks
        // until the reader reads a message.
        //
        match self.tx_que.blocking_send(value) {
            Ok(_) => Ok(()),
            Err(err) => Err(anyhow!("Error posting to send que:{:?}", err)),
        }
    }

    fn shutdown(&self) {
        let fname = "PushPromExporter::shutdown";
        log::info!("{fname} ...");

        let msg = format!("{fname}: posting Shutdown to queue...");
        #[cfg(test)]
        {
            println!("{msg}");
        }
        log::info!("{msg}");

        match self.tx_que.blocking_send(CounterId::Shutdown) {
            Ok(_) => {}
            Err(err) => {
                println!("{fname}: Error posting Shutdown to send que:{:?}", err);
            }
        };
        let msg = format!("{fname}: Waiting for reader to drain the queue...");
        #[cfg(test)]
        {
            println!("{msg}");
        }
        log::info!("{msg}");

        let tx_que = self.tx_que.clone();
        self.runtime.handle().spawn(async move {
            tx_que.closed().await;
        });

        // Wait until reader thread posts to the shutdown flag.
        let _is_shutdown = crate::wait_for!(self.shutdown);
        let msg = format!("{fname}: Waiting for reader to drain the queue...done");
        #[cfg(test)]
        {
            println!("{msg}");
        }
        log::debug!("{msg}");
    }
}

/// Concrete GenericPrometheusPushExportor that does nothing, and always returns
/// success. Kind of like a write only disk drive, it's lighting fast too.
struct NullPrometheusPushExportor {}

impl GenericPrometheusPushExportor for NullPrometheusPushExportor {
    fn increment(&self, _: CounterId) -> Result<()> {
        Ok(())
    }
    fn shutdown(&self) {}
}

pub fn create_push_exporter(
    threads: usize,
    queue_depth: usize,
    url: &str,
    data_type: &str,
    file_format: &str,
    graf_label_prefix: &str,
    config_hash: &str,
) -> anyhow::Result<Arc<dyn GenericPrometheusPushExportor>> {
    let mut graf_label: String = graf_label_prefix.to_string();

    let container_name_env = crate::util::get_container_name_env();

    match container_name_env {
        Some(s) => {
            log::info!("The container name is: {}", s);
            let cloned_string = s.clone();
            graf_label = cloned_string;
        }
        None => log::info!("The container name is None"),
    }

    match PushPromExporter::new(
        threads,
        queue_depth,
        url,
        data_type,
        file_format,
        &graf_label,
        config_hash,
    ) {
        Ok(pexp) => Ok(Arc::new(pexp)),
        Err(err) => {
            log::info!(
                "Failed to connect to Prometheus Push Gateway:{err}\n\
                        Using null PrometheusPushExportor instead."
            );
            Ok(Arc::new(NullPrometheusPushExportor {}))
        }
    }
}

#[cfg(test)]
mod tests {
    // Import names from outer scope.
    use super::*;
    const GW_URL: &str = "http://127.0.0.1:9094/metrics";
    const GRAF_PRE: &str = "GRAF-";

    #[test]
    fn connect_and_push_metrics() -> Result<()> {
        let pe = create_push_exporter(
            10,
            50,
            GW_URL,
            "Gzip",
            GRAF_PRE,
            "text/plain",
            "config-hash",
        )?;

        pe.increment(CounterId::CompressedFileCount(1, "Fail".to_string()))?;
        pe.increment(CounterId::DecompressedFileCount(2, "Ok".to_string()))?;
        pe.shutdown();
        Ok(())
    }

    #[test]
    fn connect_and_push_metrics_stress() -> Result<()> {
        let pe =
            create_push_exporter(10, 50, GW_URL, "Gzip", GRAF_PRE, "text/html", "config-hash")?;

        for _ in 0..100 {
            pe.increment(CounterId::CompressedFileCount(1, "Ok".to_string()))?;
        }
        pe.shutdown();
        Ok(())
    }

    #[test]
    fn null_pm_exporter() -> Result<()> {
        let pe = NullPrometheusPushExportor {};

        for _ in 0..100 {
            pe.increment(CounterId::CompressedFileCount(1, "Ok".to_string()))?;
        }
        pe.shutdown();
        Ok(())
    }
}
