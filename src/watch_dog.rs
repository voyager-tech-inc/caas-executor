// SPDX-FileCopyrightText: 2025 Voyager Technologies, Inc.
//
// SPDX-License-Identifier: MIT

use crate::get_args;
use anyhow::anyhow;
use sqlx::migrate::Migrator;
use sqlx::postgres::PgPoolOptions;
use sqlx::{Pool, Postgres};
use std::path::Path;
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::sync::Once;

// Arc is Async Reference Counted smart pointer
type CompressionMode = crate::db_sqlx::CompressionMode;
pub type WatchdogPtr = Arc<dyn Watchdog>; // dyn ~= ptr to a abstract base class (trait)
type AsyncFlag = Arc<AtomicBool>;

pub enum Mode {
    #[allow(unused)]
    Test(String), // not used in production
    Production,
}

pub fn create_watchdog() -> WatchdogPtr {
    // First make an attempt to create a WatchdogImpl, if that fails (probably because
    // there is no database running) fallback to a null database version.
    //
    match WatchdogImpl::new_arc() {
        Ok(wd) => wd,
        Err(err) => {
            log::info!(
                "Failed to connect to database:{err}\n  \
                        Watchdog will not use a DB connection"
            );

            let got_one = Arc::new(AtomicBool::new(false));
            Arc::new(NodatabaseWatchdog { got_one })
        }
    }
}

// Maps a std::Result to an anyhow::Result
macro_rules! map_result {
    ($from: expr) => {{
        match $from {
            Ok(__) => anyhow::Result::Ok(()),
            Err(e) => anyhow::Result::Err(anyhow!("{}", e)),
        }
    }};
}

/// Generic Watchdog trait (abstract base class)
pub trait Watchdog {
    // This is used for unit test only, see tests below.
    #[allow(unused)]
    fn got_signal(&self) -> bool;
    fn run(&self, mode: &Mode) -> anyhow::Result<()>;
}

struct NodatabaseWatchdog {
    got_one: AsyncFlag,
}

impl Watchdog for NodatabaseWatchdog {
    fn run(&self, mode: &Mode) -> anyhow::Result<()> {
        let (mut command, is_test) = match mode {
            Mode::Production => {
                let mut command = Command::new("caas-executor");
                command.arg("--is-child").args(std::env::args_os().skip(1));
                (command, false)
            }
            Mode::Test(seconds) => {
                let mut command = Command::new("/bin/sleep");
                command.arg(seconds);
                (command, true)
            }
        };
        let mut child = command.spawn()?;
        let child_id = child.id();

        ONCE_FLAG.call_once(|| {
            set_signal_handler(self.got_one.clone(), child_id);
        });

        if is_test {
            std::thread::Builder::new().spawn(move || {
                // Thread will sleep for 1/4 second then send SIGTERM to self.  This
                // mimics what "docker stop" would do in sending to process id 1 which
                // is the container entrypoint process
                //
                std::thread::sleep(std::time::Duration::from_millis(250));
                send_signal(std::process::id());
            })?;
        }

        match child.wait() {
            Ok(_) => {
                log::info!("WATCH_DOG: write DB finish (nulldb)");
                Ok(())
            }
            Err(err) => Err(anyhow!("WatchDog::child wait failed:{err}")),
        }
    }
    fn got_signal(&self) -> bool {
        self.got_one.load(Ordering::SeqCst)
    }
}

pub struct WatchdogImpl {
    pool: Pool<Postgres>,
    runtime: tokio::runtime::Runtime,
    got_one: AsyncFlag,
}

impl WatchdogImpl {
    fn new_arc() -> anyhow::Result<WatchdogPtr> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1_usize)
            .enable_io()
            .enable_time()
            .build()?;

        let db_url = crate::util::get_db_url();
        log::debug!("WatchdogImpl::new url={db_url}");

        let future = runtime.block_on(Self::async_get_connection_pool(&db_url, 1));
        let pool = match future {
            Ok(pool) => pool,
            Err(err) => {
                return Err(anyhow!("Database connect failed:{err}"));
            }
        };

        runtime.block_on(async {
            let migrator = Migrator::new(Path::new("./migrations"))
                .await
                .expect("failed to find migrations");
            migrator
                .run(&pool)
                .await
                .expect("failed to apply migrations");
        });

        let got_one = Arc::new(AtomicBool::new(false));
        let ptr = Arc::new(Self {
            pool,
            runtime,
            got_one,
        });
        Ok(ptr)
    }

    async fn async_get_connection_pool(url: &str, count: u32) -> anyhow::Result<Pool<Postgres>> {
        match PgPoolOptions::new()
            .max_connections(count)
            .connect(url)
            .await
        {
            Ok(pool) => Ok(pool),
            Err(err) => Err(anyhow!("Error getting SQLx connection pool:{}", err)),
        }
    }

    /// Inserts a new container record or updates an existing record if the same container
    /// is starting again. If it is an update only the following fields will be changed
    /// container_id, start_time, stop_time, container_args, output_dir.
    /// NOTE: stop_time becomes NULL since container has started again.
    /// Primary key is config_hash
    async fn async_insert_update_row(&self, cont: &Container) -> anyhow::Result<()> {
        log::debug!("insert_update_row: {:?}", &cont);
        let now = chrono::Utc::now().naive_utc();

        map_result!(
            sqlx::query!(
                r#"
INSERT INTO containers
(config_hash,
 command_template,
 container_id,
 container_type,
 mode,
 datatype_fileformat,
 compression_algorithm,
 is_server,
 start_time,
 container_args,
 input_directory,
 output_directory,
 grafana_label)
VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
ON CONFLICT (config_hash)
DO UPDATE SET
  container_id     = $3,
  start_time       = $9,
  stop_time        = NULL,
  container_args   = $10,
  output_directory = $12,
  grafana_label    = $13;
"#,
                cont.config_hash,
                cont.command_template,
                cont.container_id,
                cont.container_type,
                cont.mode.clone() as CompressionMode,
                cont.datatype_fileformat,
                cont.compression_algorithm,
                cont.is_server,
                now,
                cont.container_args,
                cont.input_directory,
                cont.output_directory,
                cont.grafana_label
            )
            .execute(&self.pool)
            .await
        )
    }

    async fn async_update_row(&self, config_hash: &str) -> anyhow::Result<()> {
        log::debug!("finish: config_hash={config_hash}");

        map_result!(
            sqlx::query!(
                "UPDATE containers SET stop_time = $2, container_id = NULL WHERE config_hash = $1",
                /* $1 */ config_hash,
                /* $2 */ chrono::Utc::now().naive_utc()
            )
            .execute(&self.pool)
            .await
        )
    }
}

impl Watchdog for WatchdogImpl {
    fn run(&self, watchdog_mode: &Mode) -> anyhow::Result<()> {
        use crate::util::get_config_hash;
        use crate::util::get_env;

        log::info!("Watchdog::run");

        // Get CLI args, remove extra space characters
        let container_args: Vec<String> = std::env::args().skip(1).collect();
        let container_args = container_args
            .iter()
            .map(|x| x.to_string() + " ")
            .collect::<String>();
        let mode = get_args().mode.clone();
        let container_name_env = crate::util::get_container_name_env();

        // Gets stuff from environment, that should be passed by host script.
        let container_id = get_env("HOSTNAME")?;
        let container_type = get_args().container_type.clone();
        let input_directory = get_args().host_input_dir.to_string();
        let output_directory = get_args().host_output_dir.to_string();
        let config_hash =
            get_config_hash(&get_args().host_input_dir, &get_args().command_template)?;

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

        let mut grafana_label = format!("{container_type}-{hash_end}");

        match container_name_env {
            Some(s) => {
                log::info!("The container name is: {}", s);
                grafana_label = s;
            }
            None => log::info!("The container name is None"),
        }

        log::debug!("Watchdog::container_id     : {}", container_id);
        log::debug!("Watchdog::container_type   : {}", container_type);
        log::debug!("Watchdog::input_directory  : {}", input_directory);
        log::debug!("Watchdog::output_directory : {}", output_directory);
        log::debug!("Watchdog::grafana_label : {}", grafana_label);

        let container = Container {
            mode,
            container_id,
            container_type,
            container_args,
            input_directory,
            output_directory,
            config_hash,
            command_template: get_args().command_template.to_string(),
            datatype_fileformat: get_args().datatype_fileformat.to_owned(),
            compression_algorithm: get_args().compression_algorithm.to_owned(),
            is_server: get_args().server_mode,
            grafana_label,
        };

        let (mut command, is_test) = match watchdog_mode {
            Mode::Production => {
                let mut command = Command::new("caas-executor");
                command.arg("--is-child").args(std::env::args_os().skip(1));
                (command, false)
            }
            Mode::Test(seconds) => {
                let mut command = Command::new("/bin/sleep");
                command.arg(seconds);
                (command, true)
            }
        };
        let mut child = command.spawn()?;
        let child_id = child.id();

        self.runtime
            .block_on(self.async_insert_update_row(&container))?;

        ONCE_FLAG.call_once(|| {
            set_signal_handler(self.got_one.clone(), child_id);
        });

        if is_test {
            std::thread::Builder::new().spawn(move || {
                // Thread will sleep for 1/4 second then send SIGTERM to self.  This
                // mimics what "docker stop" would do in sending to process id 1 which
                // is the container entrypoint process
                //
                std::thread::sleep(std::time::Duration::from_millis(250));
                send_signal(std::process::id());
            })?;
        }

        // Wait for child to exit.
        child.wait()?;

        log::info!("WATCH_DOG: write DB finish");

        // Update the record to indicate container has stopped and record the stop time.
        self.runtime
            .block_on(self.async_update_row(&container.config_hash))
    } // run
    fn got_signal(&self) -> bool {
        self.got_one.load(Ordering::SeqCst)
    }
}

#[derive(Debug)]
pub struct Container {
    mode: CompressionMode,
    config_hash: String, // Unique primary key e.g., input directory and command template
    command_template: String,
    container_id: String,          // docker assigned container ID
    container_type: String,        // Python host script name with no .py suffix
    datatype_fileformat: String,   // The type of data being compressed: e.g.: video/mp4
    compression_algorithm: String, // Compression algorithm: e.g.: gzip, lz4, lzma
    is_server: bool,               // Indicates running in server mode, otherwise one shot mode
    container_args: String,        // Full command line passed to container
    input_directory: String,       // Host input directory
    output_directory: String,      // Host output directory
    grafana_label: String,         // Name of the container
}

static ONCE_FLAG: Once = Once::new();

// The child_pid is used to forward the signal to the child when it is caught
fn set_signal_handler(flag: AsyncFlag, child_pid: u32) {
    let fname = "WATCH_DOG";

    // Pass "--show-output" to "cargo test" to see this output when running test. It will
    // appear in "docker logs" as well.
    //
    println!(
        "{fname}: set_signal_handler child_pid {child_pid}, my pid {}",
        std::process::id()
    );

    let status = ctrlc::set_handler(move || {
        println!("{fname}: Caught Signal");
        log::info!("{fname}: Caught Control-C, sending SIGTERM to pid:{child_pid}");
        flag.store(true, Ordering::SeqCst);
        send_signal(child_pid);
    });

    if let Err(err) = status {
        log::warn!("{fname}: Error setting Control-C handler: {err}");
    }
}

fn send_signal(pid: u32) {
    println!("WATCH_DOG: send_signal to {pid}");

    let child = Command::new("/bin/kill")
        .arg("-TERM")
        .arg(format!("{pid}"))
        .spawn();

    match child {
        Ok(_) => {}
        Err(err) => {
            log::error!("WATCH_DOG: Error sending signal:{err}");
        }
    }
}

#[cfg(test)]
mod tests {
    // Import all names from outer scope.
    use super::*;
    use serde::Deserialize;

    // Test will::
    // 1) Start child process running sleep for 10 seconds,
    // 2) sleep for 1/4 second then send SIGTERM to self (same process)
    // 3) Signal handler forwards the signal to the child.
    // 4) The child then exits and the watchdog will return from the child.wait().
    // 5) Test fails if it waits the full 10 seconds :(
    fn child_sleep_time() -> String {
        "10".to_string()
    }

    // Queries containers table and returns a Vector of rows found, given a config_hash
    // which is the primary key, it should return 0 or 1 rows.
    async fn get_container_info(pool: &Pool<Postgres>, config_hash: &str) -> ContainerRow {
        let rows = sqlx::query_as!(
            ContainerRow,
            r#"
    SELECT
      config_hash,
      container_id,
      container_type,
      mode as "mode: CompressionMode",
      datatype_fileformat,
      compression_algorithm,
      is_server,
      start_time,
      stop_time,
      container_args,
      input_directory,
      output_directory,
      grafana_label
      FROM containers
      WHERE config_hash = $1"#,
            config_hash
        )
        .fetch_all(pool)
        .await;
        rows.unwrap()[0].clone()
    }

    // Row struct maps to the database schema for containers table. Used in the tests
    // below
    #[derive(Clone, Debug, sqlx::FromRow, Deserialize)]
    #[allow(non_snake_case, dead_code)]
    pub struct ContainerRow {
        config_hash: String,
        container_id: Option<String>,
        container_type: String,
        mode: CompressionMode,
        datatype_fileformat: String,
        compression_algorithm: String,
        is_server: bool,
        start_time: sqlx::types::chrono::NaiveDateTime,
        stop_time: Option<sqlx::types::chrono::NaiveDateTime>,
        container_args: String,
        input_directory: String,
        output_directory: String,
        grafana_label: String,
    }

    #[test]
    fn watchdog_test() -> anyhow::Result<()> {
        let _ = std::env::set_var("HOSTNAME", "caas-test");
        use crate::ARGS;
        use clap::Parser;
        use sqlx::migrate::Migrator;
        use std::path::Path;

        #[rustfmt::skip]
        let mock_args = crate::arg_parser::Args::parse_from([
            "caas-executor",
            "--input-dir", "/data/input_dir",
            "--output-dir", "/data/output_dir",
            "--processed-dir", "/data/processed_dir",
            "--config-file", "/config/config_file",
            "--max-file-size-mb", "101",
            "--jobs", "102",
            "--suffix-compressed", "suffix_compressed",
            "--directory-events", "close-write",
            "--comp-mode", "decompress",
            "--exp-queue-depth", "103",
            "--exp-url", "exp_url",
            "--exp-threads", "104",
            "--db-threads", "105",
            "--db-connections", "106",
            "--datatype-fileformat", "datatype_fileformat",
            "--grafana-label-prefix", "graf-",
            "--compression-algorithm", "compression_algorithm",
            "--server-mode",
            "--is-child",
            "--container-type", "caas-gzip",
            "--host-input-dir", "/data/input_dir",
            "--host-output-dir", "/data/output_dir",
            "caas-gzip --mode decompress --infile=%{IN} --outfile=%{OUT} --type=iq",
        ]);

        ARGS.set(mock_args).ok();

        println!("{:?}", *get_args());

        let config_hash =
            crate::util::get_config_hash(&get_args().host_input_dir, &get_args().command_template)?;

        println!("WATCH_DOG: config_hash :: {config_hash}");

        let watchdog = WatchdogImpl::new_arc()?;

        let start_time = std::time::Instant::now();
        watchdog.run(&Mode::Test(child_sleep_time()))?;

        // Verify that signal was indeed captured by the watchdog
        assert!(watchdog.got_signal(), "Child didn't get SIGTERM ");

        let delta_time = std::time::Instant::now() - start_time;

        // If time delta was greater than 1 sec then fail the test.
        assert!(
            delta_time < std::time::Duration::from_secs(1),
            "Child didn't exit or didn't get SIGTERM "
        );

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1_usize)
            .enable_io()
            .enable_time()
            .build()?;

        let db_url = crate::util::get_db_url();
        let future = runtime.block_on(WatchdogImpl::async_get_connection_pool(&db_url, 1));
        let pool = match future {
            Ok(pool) => pool,
            Err(err) => {
                return Err(anyhow!("Database connect failed:{err}"));
            }
        };

        runtime.block_on(async {
            let migrator = Migrator::new(Path::new("./migrations"))
                .await
                .expect("failed to find migrations");
            migrator
                .run(&pool)
                .await
                .expect("failed to apply migrations");
        });

        let record = runtime.block_on(get_container_info(&pool, config_hash.as_str()));

        let today = chrono::Utc::now().naive_utc().date();
        assert_eq!(record.config_hash, config_hash);
        assert_eq!(record.start_time.date(), today);
        assert_eq!(record.container_id, None);
        assert_eq!(record.container_type, "caas-gzip".to_string());
        assert_eq!(record.mode, CompressionMode::Decompress);
        assert_eq!(record.datatype_fileformat, get_args().datatype_fileformat);
        assert_eq!(
            record.compression_algorithm,
            get_args().compression_algorithm
        );
        assert_eq!(record.is_server, get_args().server_mode);
        assert_eq!(record.stop_time.unwrap().date(), today);
        println!("WATCH_DOG: container_get_args():{}", record.container_args);
        assert_eq!(record.input_directory, get_args().input_dir);
        assert_eq!(record.output_directory, get_args().output_dir);

        // This second watchdog run tests the case were a container is run a second time
        // with the same command_template, that is it should it update the database record
        // and not get an insertion error on duplicate primary key.
        let watchdog = WatchdogImpl::new_arc()?;
        watchdog.run(&Mode::Test("1".to_string()))?;

        let record = runtime.block_on(get_container_info(&pool, config_hash.as_str()));

        println!("WATCH_DOG: {:#?}", record);
        Ok(())
    }
} // end of test
