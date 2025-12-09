// SPDX-FileCopyrightText: 2025 Voyager Technologies, Inc.
//
// SPDX-License-Identifier: MIT

use anyhow::{anyhow, Result};
use clap::Parser;
use once_cell::sync::OnceCell;
use std::sync::{Arc, Condvar, Mutex};

type ShutdownFlag = Arc<(Mutex<bool>, Condvar)>;

mod app;
mod arg_parser;
mod db_sqlx;
mod dir_mon;
mod file_processor;
mod path_bundle;
mod pm_exporter;
mod util;
mod watch_dog;

use arg_parser::Args;

static ARGS: OnceCell<Args> = OnceCell::new();

#[macro_export]
macro_rules! wait_for {
    ($value: expr) => {{
        // For Condvar usage see:
        // https://doc.rust-lang.org/std/sync/struct.Condvar.html
        let (lock, cvar) = &*$value;
        let mut value = lock.lock().unwrap();
        while !*value {
            value = cvar.wait(value).unwrap();
        }
        value
    }};
}

#[macro_export]
macro_rules! set_flag {
    ($value: expr) => {{
        let (lock, cvar) = &*$value;
        let mut value = lock.lock().unwrap();
        *value = true;

        // Notify the waiter
        cvar.notify_one();
    }};
}

pub fn get_args() -> &'static Args {
    ARGS.get().expect("app args were not initialized")
}

fn main() -> Result<()> {
    let args = Args::parse();
    ARGS.set(args).unwrap();
    init_logger(get_args().fancy_logs);

    is_valid_command(&get_args().command_template)?;

    if !get_args().is_child {
        // This is the top level caas-executor, it will launch the real working and then
        // monitor it and wait for it to exit.
        //
        log::debug!(
            "This is the 'watch dog' process and my pid is {}...",
            std::process::id()
        );

        let watch_dog = watch_dog::create_watchdog();

        let status = watch_dog.run(&watch_dog::Mode::Production);

        log::info!(
            "MAIN: watch_dog.run() returned:{:?}, got_signal:{}",
            status,
            watch_dog.got_signal()
        );

        return status;
    }

    if log::log_enabled!(log::Level::Debug) {
        log::debug!("{:?}", *get_args());
    }

    let shutdown = Arc::new((Mutex::new(false), Condvar::new()));

    set_signal_handler(shutdown.clone());

    // Create the Application context
    let app = app::Application::new()?;
    let app = Arc::new(app);

    if get_args().dry_run {
        log::info!("dry-run mode");
        app.log();
        log::info!("Command Template: '{}'", get_args().command_template);
        if app.check_mount_points() {
            return Ok(());
        } else {
            return Err(anyhow!("Mount points are invalid."));
        }
    }

    let app_2 = app.clone(); // Clone to pass to app.run()
    let shut_2 = shutdown.clone(); // Clone to pass closure for shutdown notify

    std::thread::Builder::new()
        .name("app-main".to_string())
        .spawn(move || {
            // Call app.run (blocking)
            match app_2.run() {
                Ok(_) => {}
                Err(err) => {
                    log::error!("Failed:{err}");
                    std::process::exit(1);
                }
            }
            set_flag!(shut_2); // set shutdown flag
        })?;

    // Wait for the shutdown flag to be set
    let _is_shutdown = wait_for!(shutdown);

    // Shutdown app thread, blocks while pending transactions are completed.
    app.shutdown();

    log::debug!("MAIN: ... done");
    #[cfg(test)]
    {
        println!("MAIN: ... done");
    }
    Ok(())
} // main

fn set_signal_handler(shutdown: ShutdownFlag) {
    let fname = "MAIN";

    log::debug!("{fname}: set_signal_handler my pid {}", std::process::id());

    let status = ctrlc::set_handler(move || {
        log::debug!("{fname}: Caught Signal");
        set_flag!(shutdown);
    });

    if let Err(err) = status {
        log::warn!("{fname}: Error setting Control-C handler: {err}");
    }
}

fn init_logger(fancy_logs: bool) {
    let env = env_logger::Env::default();

    let style = if fancy_logs {
        // Colorize if output is TTY
        env_logger::WriteStyle::Auto
    } else {
        // Never colorize
        env_logger::WriteStyle::Never
    };

    env_logger::Builder::from_env(env)
        .format_level(true) // include the log level
        .format_timestamp_millis()
        .write_style(style)
        .target(env_logger::Target::Stderr)
        .init();
}

fn is_valid_command(cmd: &str) -> Result<()> {
    if cmd.is_empty() || !cmd.contains("%{IN}") || !cmd.contains("%{OUT}") {
        return Err(anyhow!(
            "Invalid template \"{}\": does not contain file tokens {}, {}.",
            cmd,
            "%{IN}",
            "%{OUT}"
        ));
    }
    Ok(())
}
