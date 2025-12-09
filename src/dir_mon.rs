// SPDX-FileCopyrightText: 2025 Voyager Technologies, Inc.
//
// SPDX-License-Identifier: MIT

use anyhow::{anyhow, Result};
use clap::ValueEnum;
use std::collections::HashSet;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::mpsc::SyncSender;
use std::sync::{Arc, Condvar, Mutex};

type MessageQueue = SyncSender<(String, String, String)>;
type ChildPid = Arc<(Mutex<u32>, Condvar)>;

macro_rules! set_child_pid {
    ($flag: expr, $value: expr) => {{
        let (lock, cvar) = &*$flag;
        let mut flag = lock.lock().unwrap();
        *flag = $value;

        // Notify the waiter
        cvar.notify_one();
    }};
}

/// Watches a directory for file close events in it or for files that are renamed into the
/// directory.
pub struct DirectoryMonitor {
    /// Message queue to send directory events to.
    sender: MessageQueue,

    /// Full path to directory being watched
    path: PathBuf,

    /// Pending file list
    pending_files: HashSet<String>,

    /// Directory events to subscribe to
    events: Event,
}

#[derive(Debug, Clone, Default, serde::Deserialize, serde::Serialize, ValueEnum)]
pub enum Event {
    /// Process moved to events from inotifywait
    MovedTo,

    /// Process close write events from inotifywait
    CloseWrite,

    /// Process close write and moved to events from inotifywait
    #[default]
    Both,
}

impl DirectoryMonitor {
    /// Creates a new class instance to monitor a path and send events to sender queue.
    pub fn new(path: PathBuf, event_type: Event, sender: MessageQueue) -> Result<Self> {
        if !path.is_dir() {
            return Err(anyhow!("{:?} is not a directory", path));
        }

        // Can we read it (no permissions issues)
        if let Err(e) = path.read_dir() {
            return Err(anyhow!("Error reading directory:{:?}: {:?}", path, e));
        }
        Ok(Self {
            sender,
            path,
            events: event_type,
            pending_files: HashSet::new(),
        })
    }

    /// Runs the event loop reading directory events and posting them to the message
    /// queue. Caller is expected spawn a thread to call this method. Does not return.
    pub fn run(&mut self, child_pid: ChildPid) {
        let events = match self.events {
            Event::MovedTo => vec!["-emoved_to"],
            Event::CloseWrite => vec!["-eclose_write"],
            Event::Both => vec!["-emoved_to", "-eclose_write"],
        };

        // Launch the child process to monitor the directory using inotifywait(1) command
        // with given event filter
        let mut child = match Command::new("inotifywait")
            .arg("-m")
            .args(events)
            .arg("-emoved_from")
            .arg("-edelete")
            .arg(self.path.as_os_str())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
        {
            Ok(val) => val,
            Err(err) => {
                log::error!(
                    "Got error launching inotifywait on {:?}: {:?}",
                    self.path,
                    err
                );
                return;
            }
        };

        log::info!("Monitoring directory:{:?}", self.path);

        let stdout = match child.stdout.take() {
            Some(stdout) => stdout,
            None => {
                panic!("Unexpected error getting stdout of child:{:?}", self.path)
            }
        };
        let stderr = match child.stderr.take() {
            Some(stderr) => stderr,
            None => {
                panic!("Unexpected error getting stderr of child:{:?}", self.path)
            }
        };
        use std::io::BufRead;
        {
            let reader = std::io::BufReader::new(stderr);
            for l in reader.lines().map_while(Result::ok) {
                if l == "Watches established." {
                    break;
                }
            }
        }

        // Do this AFTER inotifywait prints "Watches established." so we know it is ready
        // for action.  This passes the child_pid back to the caller of this class
        set_child_pid!(child_pid, child.id());

        // Output of inotifywait(1) is <DIR> <EVENT> <FILE>
        let reader = std::io::BufReader::new(stdout);
        for line in reader.lines() {
            match line {
                Ok(line) => {
                    log::trace!("run: got line {line}");
                    let vec: Vec<&str> = line.split(' ').collect();
                    if vec.len() != 3 {
                        log::warn!("run: invalid event syntax {line}");
                        continue;
                    }
                    let dir = vec[0];
                    let event = vec[1];
                    let file = vec[2];
                    if self.process_event(dir, event, file) {
                        break;
                    }
                }
                Err(x) => {
                    log::error!("run: Child read error: {:?} on {:?}", x, self.path);
                }
            }
        }
        log::debug!("run: exiting dir mon for path={:?}", self.path);
    }

    fn process_event(&mut self, dir: &str, event: &str, file: &str) -> bool {
        match event {
            "MOVED_FROM" | "DELETE" => {
                if self.pending_files.remove(file) {
                    log::debug!(
                        "process_event: got {event} for file:{file}, \
                                 deleted from pending_files, dir:{dir}"
                    );
                } else {
                    log::warn!(
                        "process_event: got {event} for file:{file} \
                         but file not found in pending_files, dir:{dir}"
                    );
                }
            }
            "MOVED_TO" | "CLOSE_WRITE,CLOSE" => {
                if self.pending_files.insert(file.to_string()) {
                    log::debug!(
                        "process_event: got {event} for file:{file}, added to \
                                 pending table, dir:{dir}"
                    );
                } else {
                    log::debug!(
                        "process_event: duplicate file {file} was discarded on \
                                 {event} event, dir:{dir}"
                    );
                    return false;
                }
                log::debug!("process_event: posting event {dir} {event} {file}");
                let msg = (dir.to_string(), event.to_string(), file.to_string());
                if self.sender.send(msg).is_err() {
                    log::debug!("process_event: got send error, reader gone?, dir:{dir}");
                    // Means reader has gone a away, tell caller so we are finished so it
                    // can exit main loop
                    return true;
                }
            }
            &_ => {
                log::warn!("process_event: unhandled event {event}, dir:{dir}");
            }
        }
        false
    } // fn process_event
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::sync::mpsc::sync_channel;

    macro_rules! wait_for {
        ($value: expr) => {{
            let (lock, cvar) = &*$value;
            let mut value = lock.lock().unwrap();
            while *value == 0 {
                value = cvar.wait(value).unwrap();
            }
            value
        }};
    }

    #[test]
    fn non_existent_dir() -> Result<()> {
        let cwd = std::env::current_dir()?;
        let cwd = cwd.display().to_string();
        let watched_dir = format!("{cwd}/_test_non_exist_dir/watched");

        assert!(!PathBuf::from(&watched_dir).is_dir());

        let (tx, _rx) = sync_channel(1);
        let dm = DirectoryMonitor::new(PathBuf::from(&watched_dir), Event::MovedTo, tx);
        if dm.is_ok() {
            Err(anyhow!("did not detect non-existent directory"))
        } else {
            purge_tree(&format!("{cwd}/_test_non_exist_dir"));
            Ok(())
        }
    }

    // Test directory being watched already exists as a regular file
    #[test]
    fn invalid_dir() -> Result<()> {
        let cwd = std::env::current_dir()?;
        let cwd = cwd.display().to_string();
        let base_dir = || format!("{cwd}/_test_id");
        let wtch_dir = || format!("{cwd}/_test_id/watched");

        println!("cwd            :{}", cwd);
        println!("create_dir_all :{}", base_dir());
        purge_tree(&base_dir());

        std::fs::create_dir_all(PathBuf::from(&base_dir()))?;
        assert!(PathBuf::from(&base_dir()).is_dir());

        touch(PathBuf::from(&wtch_dir()))?;
        assert!(PathBuf::from(&wtch_dir()).is_file());

        let (tx, _rx) = sync_channel(10);
        if DirectoryMonitor::new(PathBuf::from(&wtch_dir()), Event::MovedTo, tx).is_ok() {
            Err(anyhow!(
                "Test failed, did not detect non-existent directory"
            ))
        } else {
            purge_tree(&base_dir());
            Ok(())
        }
    }

    // Test directory being watched exists but caller does not have access permissions.
    #[test]
    fn no_perms_on_watch_dir() -> Result<()> {
        let cwd = std::env::current_dir()?;
        let cwd = cwd.display().to_string();
        let base_dir = || format!("{cwd}/_test_np");
        let wtch_dir = || format!("{cwd}/_test_np/watched");

        println!("cwd            :{}", cwd);
        println!("create_dir_all :{}", base_dir());
        purge_tree(&base_dir());

        std::fs::create_dir_all(PathBuf::from(&base_dir()))?;
        assert!(PathBuf::from(&base_dir()).is_dir());

        touch(PathBuf::from(&wtch_dir()))?;
        assert!(PathBuf::from(&wtch_dir()).is_file());

        chmod(&wtch_dir(), 0)?;

        let (tx, _rx) = sync_channel(10);
        if DirectoryMonitor::new(PathBuf::from(&wtch_dir()), Event::CloseWrite, tx).is_ok() {
            Err(anyhow!(
                "Test failed, did not detect directory w/o rwx permissions"
            ))
        } else {
            purge_tree(&base_dir());
            Ok(())
        }
    }

    #[test]
    fn sunny_day_close_write() -> Result<()> {
        sunny_day("cw", Event::CloseWrite)
    }

    #[test]
    fn sunny_day_move_to() -> Result<()> {
        sunny_day("mt", Event::MovedTo)
    }

    // A simple implementation of `% touch path` (ignores existing files)
    fn touch(path: PathBuf) -> Result<()> {
        match std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(path.clone())
        {
            Ok(_) => Ok(()),
            Err(e) => Err(anyhow!("Touch failed:{:?}:{:?}", path, e)),
        }
    }

    // A simple implementation of `% chmod xxx path`
    fn chmod(path: &str, perms: u8) -> Result<()> {
        std::process::Command::new("chmod")
            .args([&format!("{}", perms), path])
            .output()
            .expect("Failed to execute process");
        Ok(())
    }

    fn make_test_data(
        file: &str,
        blks: usize,
        blksize: usize,
        move_to: bool,
        cwd: &str,
    ) -> Result<()> {
        use std::fs::File;
        use std::io::Write;

        let do_create = |file: &str| -> Result<()> {
            let mut file_x = File::create(PathBuf::from(file))?;
            let mut data = Vec::<u8>::new();

            data.resize(blksize, b'a');

            for _ in 0..blks {
                file_x.write_all(&data)?;
            }
            Ok(())
        };
        println!("make_test_data:{}", file);

        if move_to {
            // Create a file named 'x' in current directory and rename it into the target
            // directory. This will test if renaming into the directory works.
            {
                let xfile = format!("{cwd}/_^_xtmp.dat");
                match do_create(&xfile) {
                    Ok(_) => {}
                    Err(err) => {
                        eprintln!("Error in create {} : {}", xfile, err);
                    }
                }
                match std::fs::rename(&xfile, file) {
                    Ok(_) => {}
                    Err(err) => {
                        eprintln!("Error in rename {} -> {} : {}", xfile, file, err);
                    }
                }
            }
        } else {
            do_create(file)?;
        }
        Ok(())
    }

    fn purge_tree(base_dir: &str) {
        // Cleanup
        std::process::Command::new("rm")
            .args(["-rf", base_dir])
            .output()
            .expect("Failed to execute process");
    }

    fn sunny_day(dir: &str, event: Event) -> Result<()> {
        let cwd = std::env::current_dir()?;
        let cwd = cwd.display().to_string() + "/" + dir;
        let base_dir = || format!("{cwd}/_test_sd");
        let mon_dir_ = || format!("{cwd}/_test_sd/watched/");
        let filename = |s: &str| format!("{cwd}/_test_sd/watched/{s}");

        purge_tree(&base_dir());

        println!("create_dir_all:{}", mon_dir_());
        std::fs::create_dir_all(PathBuf::from(&mon_dir_()))?;
        assert!(PathBuf::from(&mon_dir_()).is_dir());

        let (tx, rx) = sync_channel(10);

        let child_pid = Arc::new((Mutex::new(0_u32), Condvar::new()));
        let child_pid_ = child_pid.clone();
        let mut mon = DirectoryMonitor::new(PathBuf::from(&mon_dir_()), event.clone(), tx)?;
        std::thread::Builder::new().spawn(move || mon.run(child_pid_))?;

        let child_pid = wait_for!(child_pid);
        assert!(*child_pid != 0_u32);
        //std::thread::sleep(std::time::Duration::from_millis(1000));

        // Four test data files
        let mut expect_files = BTreeMap::new();
        expect_files.insert("001", (1, 1024));
        expect_files.insert("002", (100, 10240));
        expect_files.insert("skip-a-few", (10, 10240));
        expect_files.insert("099", (15, 10240));
        expect_files.insert("100", (1, 1024));

        let is_move_to = dir == "mt";
        for (key, value) in expect_files.iter() {
            make_test_data(&filename(key), value.0, value.1, is_move_to, &cwd)?;
        }

        let expect = match &event {
            Event::CloseWrite => "CLOSE_WRITE,CLOSE",
            Event::MovedTo => "MOVED_TO",
            Event::Both => "BOTH",
        };

        for _n in 0..expect_files.len() {
            // Get next message from receive end of the message queue
            let (dir, event, name) = rx.recv()?;
            assert_eq!(dir, mon_dir_());
            assert_eq!(event, expect);
            println!("Event:{event} name:{name} path:{dir}");
            assert_eq!(true, expect_files.contains_key(name.as_str()));
        }

        let mut child = Command::new("/bin/kill")
            .arg("-TERM")
            .arg(&format!("{child_pid}"))
            .spawn()
            .unwrap();

        match child.wait() {
            Ok(_) => {
                println!("Kill inotifywait:{child_pid} Ok");
            }
            Err(err) => {
                log::error!("shutdown: Error sending signal:{err}");
            }
        }

        let cwd = std::env::current_dir()?;
        let cwd = cwd.display().to_string() + "/" + dir;
        purge_tree(&cwd);

        Ok(())
    }
}
