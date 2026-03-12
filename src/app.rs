// SPDX-FileCopyrightText: 2025 Voyager Technologies, Inc.
//
// SPDX-License-Identifier: MIT

use super::dir_mon::*;
use anyhow::{anyhow, Result};
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::Command;
use std::sync::{Arc, Condvar, Mutex};
use threadpool::ThreadPool;

use super::path_bundle::PathBundle;
use super::pm_exporter::*;

use crate::get_args;

type PushPromExporterPtr = Arc<dyn GenericPrometheusPushExportor>; // shared_ptr
type DatabasePtr = Arc<dyn super::db_sqlx::Database>;
type ChildPid = Arc<(Mutex<u32>, Condvar)>;

macro_rules! load_child_pid {
    ($value: expr) => {{
        // For Condvar usage see:
        // https://doc.rust-lang.org/std/sync/struct.Condvar.html
        let (lock, cvar) = &*$value;
        // TODO, Address this unwrap.
        let mut value = lock.lock().unwrap();
        while *value == 0 {
            value = cvar.wait(value).unwrap();
        }
        value
    }};
}

/// Application context class, a singleton (though not enforced).
pub struct Application {
    prom_exporter: PushPromExporterPtr,
    db: DatabasePtr,
    compression_algorithm: String,
    thread_pool: ThreadPool,
    child_pid: ChildPid,
    shutdown_ack: crate::ShutdownFlag,
}

impl Application {
    /// Application constructor
    pub fn new() -> Result<Self> {
        // Compute the config_hash
        let config_hash =
            crate::util::get_config_hash(&get_args().host_input_dir, &get_args().command_template)?;

        // Create the database instance, stash it in a shared smart pointer.
        let db = super::db_sqlx::create_database(
            get_args().db_connections,
            get_args().db_threads,
            &config_hash,
        )?;

        // Wait for watch dog to insert 'container started' record to containers DB table.
        db.wait_for_container_record()?;

        let compression_algorithm = get_args().compression_algorithm.clone();

        // Create prom exporter instance to push metrics to our Prometheus gateway.
        let prom_exporter = super::pm_exporter::create_push_exporter(
            get_args().exp_threads,          // threads
            get_args().exp_queue_depth,      // queue_depth
            &get_args().exp_url,             // url
            &compression_algorithm,          // data_type
            &get_args().datatype_fileformat, // file_format
            &get_args().graf_label_prefix,   // graf_label_prefix
            &config_hash,                    // config_hash
        )?;

        // Thread pool with max threads == jobs.
        let thread_pool = ThreadPool::with_name("caas_executor".to_string(), get_args().jobs);

        // The DirectoryMonitor will pass back the PID inotifywait process here
        let child_pid = Arc::new((Mutex::new(0_u32), Condvar::new()));
        let shutdown_ack = Arc::new((Mutex::new(false), Condvar::new()));

        let app = Self {
            prom_exporter,
            db,
            compression_algorithm,
            thread_pool,
            child_pid,
            shutdown_ack,
        };

        // Create the processed directory
        if let Some(processed_dir) = &app.dir_proc() {
            ensure_dir_exists(processed_dir)?;
        }

        if get_args().use_hidden_files {
            // Create .caas directory for use-hidden-files mode
            let hidden_path = PathBuf::from(&(app.path_out().to_owned())).join(".caas");
            ensure_dir_exists(&hidden_path)?;
        }

        Ok(app)
    }

    pub fn run(&self) -> Result<()> {
        self.log();

        log::debug!("run server_mode={}", get_args().server_mode);

        use std::sync::mpsc::sync_channel;

        let max_size_mb = self.max_size_mb() as f32;
        let config_file = self.config_file();
        let suffix_skip = self.suffix_skip();

        // A queue of length 10 should be enough since there is a pipe between the
        // inotifywait child process and this one which will also buffer some strings
        // and it's no big deal if inotifywait has to block.
        let (tx, rx) = sync_channel(10);
        let path = std::path::PathBuf::from(self.path_in());
        let mut mon = DirectoryMonitor::new(path, get_args().directory_events.clone(), tx)?;

        // Clone of child_pid to pass to closure below
        let child_pid = self.child_pid.clone();

        // Only if we're in server mode mash the start button on the dir monitor.
        if get_args().server_mode {
            // The closure takes ownership of parameters it captures, MON already owns TX
            // end of the message queue. The RX end of mqueue is used in the server mode
            // loop below.
            std::thread::Builder::new()
                .name("fta-dir-mon".to_string())
                .spawn(move || mon.run(child_pid))?;

            // Wait for child pid to be set by MON thread
            let child_pid = load_child_pid!(self.child_pid);
            log::debug!("run inotifywait pid:{child_pid}");
        }

        let files_found = self.process_existing_files()?;
        if !get_args().server_mode && !files_found {
            let msg = format!("No files found in input dir:{}", self.path_in());
            log::debug!("run: {msg}");
            return Err(anyhow!("{msg}"));
        }

        // Run server mode if it is enabled
        if get_args().server_mode {
            log::debug!("run: Entering server_mode loop...");
            loop {
                let value = rx.recv();
                if value.is_err() {
                    // Not an error if we are shutting down you know
                    log::debug!("run() recv returns None...");
                    break;
                }
                let (dir, _event, name) = value.unwrap();
                let mut fullpath = std::path::PathBuf::from(dir);

                fullpath.push(name);

                if let Some(name) = super::util::is_valid_file(
                    fullpath.as_path(),
                    max_size_mb,
                    config_file,
                    suffix_skip,
                ) {
                    log::debug!("run: server_mode loop schedule_file:{name}");
                    self.schedule_file(name.to_owned());
                } else {
                    log::debug!("run: server_mode loop, not a valid file:{:?}", fullpath);
                }
            }
        }
        log::debug!("run() processed all files");

        self.join_thread_pool()?;
        self.prom_exporter.shutdown();

        crate::set_flag!(self.shutdown_ack);

        log::debug!("run() FINISHED");
        Ok(())
    }

    pub fn directory_monitor_shutdown(&self) {
        if !get_args().server_mode {
            return;
        }

        log::debug!("directory_monitor_shutdown");

        let pid = load_child_pid!(self.child_pid);

        log::debug!("shutdown: sending SIGTERM to inotifywait :{pid}");

        let child = Command::new("/bin/kill")
            .arg("-TERM")
            .arg(format!("{pid}"))
            .spawn();

        match child {
            Ok(_) => {}
            Err(err) => {
                log::error!("shutdown: Error sending signal:{err}");
            }
        }
    }

    pub fn shutdown(&self) {
        log::debug!("shutdown");
        #[cfg(test)]
        println!("shutdown");

        self.directory_monitor_shutdown();

        log::debug!("shutdown:: waiting for shutdown_ack");

        let _not_used = crate::wait_for!(self.shutdown_ack);

        let msg = "shutdown:: waiting for shutdown...done";
        log::debug!("{msg}");
        #[cfg(test)]
        println!("{msg}");
    }

    // Process all files that existed before directory monitor got started up. TODO: There
    // is a race condition so there is a possibility of duplicate files.
    fn process_existing_files(&self) -> Result<bool> {
        log::debug!("process_existing_files");

        // Get the list of files to be processed from PATH IN.
        let files = match super::util::get_file_list(
            self.path_in(),
            self.max_size_mb(),
            self.config_file(),
            self.suffix_skip(),
        ) {
            Err(err) => {
                // Got a directory read error of some kind.
                return Err(anyhow!("Error reading input directory: {:?}", err));
            }
            Ok(files) => files,
        };

        if files.is_empty() {
            return Ok(false);
        }
        let max_size_mb = self.max_size_mb() as f32;
        let path_in = self.path_in();

        // Post a processing command for each file to the thread pool for execution.
        //
        for basename in files {
            let mut file_path = std::path::PathBuf::from(path_in);
            file_path.push(basename);

            if let Some(name) = super::util::is_valid_file(
                file_path.as_path(),
                max_size_mb,
                self.config_file(),
                self.suffix_skip(),
            ) {
                log::debug!("process_existing_files schedule_file:{:?}", file_path);
                self.schedule_file(name.to_owned());
            }
        }
        log::debug!("process_existing_files...Done");
        Ok(true)
    }

    /// Schedule a file processing task for execution on the thread pool
    fn schedule_file(&self, basename: String) {
        log::debug!("schedule_file:: {basename}");

        let (command_line, path_bundle) = self.make_command(&basename);

        // These are not deep copies, the cloner will increment the reference count of the
        // respective smart pointers to passed an "owned" reference to the thread.
        //
        let prom_exporter = self.prom_exporter.clone();
        let db = self.db.clone();

        let uuid = uuid::Uuid::new_v4().to_string();
        let mode = self.mode();
        let suffix = self.suffix().to_string();

        // The move keyword below will 'move' the referenced variables into the closure
        // (lambda) so they will be available in the worker thread after they go out of
        // scope in this loop.  Like C++ the closure has hidden copies of them.
        //
        self.thread_pool.execute(move || {
            let mut file_processor = super::file_processor::FileProcessor::new(
                path_bundle,
                suffix,
                uuid,
                command_line,
                mode,
                db,
                prom_exporter,
            );
            if let Err(err) = file_processor.process_file() {
                log::warn!("schedule_file: Processing failed for file:{basename} with error:{err}");
            }
        });
    }

    fn join_thread_pool(&self) -> Result<()> {
        if log::log_enabled!(log::Level::Debug) {
            loop {
                let queued = self.thread_pool.queued_count();
                let active = self.thread_pool.active_count();

                log::debug!("Queued/Active : {}/{}", queued, active);

                if queued == 0 && active == 0 {
                    break;
                }

                let time = std::time::Duration::from_millis(1000);
                std::thread::sleep(time);
            }
        }

        // Join the pool to wait for all jobs to complete.
        self.thread_pool.join();

        let err_count = super::file_processor::get_error_count();

        if err_count > 0 {
            Err(anyhow!("{} files had failures", err_count))
        } else {
            Ok(())
        }
    }

    /// Generate a full path to the file in the processed dir.
    fn dir_proc(&self) -> Option<PathBuf> {
        let processed_dir = &get_args().processed_dir;

        if processed_dir != "None" {
            let dir_proc = std::path::PathBuf::from(processed_dir);

            if dir_proc.is_absolute() {
                Some(dir_proc)
            } else {
                let path = PathBuf::from(self.path_in());
                Some(path.join(processed_dir))
            }
        } else {
            None
        }
    }

    pub fn log(&self) {
        log::info!("path_in     : {}", self.path_in());
        log::info!("path_out    : {}", self.path_out());
        log::info!("command     : {}", self.command());
        log::info!("config      : {}", self.config_file());
        log::info!("suffix-skip : {}", self.suffix_skip());
        log::info!("suffix      : {}", self.suffix());
        log::info!("data_type   : {}", self.compression_algorithm);
        log::info!("file_format : {}", self.file_format());
        log::info!("server_mode : {}", get_args().server_mode);

        if self.max_size_mb() == (usize::MAX) {
            log::info!("max_size : unlimited");
        } else {
            log::info!("max_size : {} MB", self.max_size_mb());
        }

        log::info!("jobs     : {}", get_args().jobs);
    }

    pub fn check_mount_points(&self) -> bool {
        if !std::path::Path::new(self.path_in()).exists() {
            log::error!("Input path {} does not exist", self.path_in());
            false
        } else if !std::path::Path::new(self.path_out()).exists() {
            log::error!("Output path {} does not exist", self.path_out());
            false
        } else {
            true
        }
    }

    /// Instantiates the command template for given file name
    fn make_command(&self, name: &str) -> (String, PathBundle) {
        //
        // This will be base name of output file and it may have a suffix.
        // The end of this assignment is at the ';' about 20 lines down
        //
        let name_out = if self.suffix().is_empty() {
            name.to_string() // No suffix... output name is same as input name
        } else {
            // We have a suffix...

            if name.ends_with(self.suffix()) {
                //
                // Input name has suffix so we are decompressing and we want to strip it
                // from output file name.
                //
                name.strip_suffix(self.suffix()).unwrap().to_string()
            } else {
                // Input name does NOT have suffix so we are compressing and we want the
                // output file name to have a suffix.
                //
                format!("{}{}", name, self.suffix())
            }
        };

        log::debug!(
            "@FILE: {}: name_out: {} use_hidden_files:{}",
            name,
            name_out,
            get_args().use_hidden_files
        );

        let filein = format!("{}/{}", self.path_in(), name);
        let fileout = if get_args().use_hidden_files {
            format!("{}/.caas/{}", self.path_out(), name_out)
        } else {
            format!("{}/{}", self.path_out(), name_out)
        };

        let command_line = self
            .command()
            .to_string()
            .replace("%{IN}", &filein)
            .replace("%{OUT}", &fileout);

        let pathbund = PathBundle {
            path_in: PathBuf::from(self.path_in().to_owned()),
            path_out: PathBuf::from(self.path_out().to_owned()),
            path_proc: self.dir_proc(),
            name_in: name.to_owned(),
            name_out,                        // basename of output file
            fileout: PathBuf::from(fileout), // full path to file out
        };
        (command_line, pathbund)
    }

    fn path_out(&self) -> &str {
        &get_args().output_dir
    }
    fn path_in(&self) -> &str {
        &get_args().input_dir
    }
    fn config_file(&self) -> &str {
        &get_args().config_file
    }
    fn suffix(&self) -> &str {
        &get_args().suffix_compressed
    }
    fn command(&self) -> &str {
        &get_args().command_template
    }
    fn max_size_mb(&self) -> usize {
        get_args().max_file_size_mb
    }
    fn mode(&self) -> super::db_sqlx::CompressionMode {
        get_args().mode.clone()
    }
    fn file_format(&self) -> &str {
        get_args().datatype_fileformat.as_str()
    }
    fn suffix_skip(&self) -> &str {
        &get_args().suffix_skip
    }
}

/// Create a directory if it does not exist. If it is created set its permissions to 0777.
/// We are in container running as root and thus files we create will be owned by root. We
/// want callers outside container to be able to manipulate the files after we're done so
/// we need rwx access for everyone on the new dir.
///
/// Return: Nothing '()' on success, else std::io::Error on failure
//
fn ensure_dir_exists(new_dir: &PathBuf) -> Result<(), std::io::Error> {
    log::debug!("ensure_dir_exists:{}", new_dir.display());
    //
    // Attempt to obtain meta data of the proposed new dir.
    //
    if !new_dir.exists() {
        //
        // Most likely error is 'does not exist', so create it.

        let new_dir_handle = fs::create_dir_all(new_dir);

        match new_dir_handle {
            Ok(_) => {
                log::info!("Created directory: {}", new_dir.display());
            }
            Err(err) => {
                log::error!("Error creating directory: {}", new_dir.display());
                return Err(err);
            }
        }

        let metadata = fs::metadata(new_dir)?;
        let mut perm = metadata.permissions();
        perm.set_mode(0o777);
        fs::set_permissions(new_dir, perm)?;
    }
    Ok(())
}
