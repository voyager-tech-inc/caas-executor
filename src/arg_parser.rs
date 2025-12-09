// SPDX-FileCopyrightText: 2025 Voyager Technologies, Inc.
//
// SPDX-License-Identifier: MIT

use clap::Parser;

type CompressionMode = super::db_sqlx::CompressionMode;
type DirectoryListenEvents = super::dir_mon::Event;

#[derive(Parser, Debug)]
#[command(version)]
pub struct Args {
    /// Input base directory (mount point)
    #[arg(short, long, default_value = "/opt/caas/data/input")]
    pub input_dir: String,

    /// Output base directory (mount point)
    #[arg(short, long, default_value = "/opt/caas/data/output")]
    pub output_dir: String,

    /// Name of processed directory where processed input files will be moved to.
    /// Assumes that processed dir is in same mount directory as the input files.
    #[arg(short, long, default_value = "processed")]
    pub processed_dir: String,

    /// Host script configuration file name so that it can skipped
    #[arg(short, long, default_value = "")]
    pub config_file: String,

    /// The maximum input file size that will be processed in MB. Files larger than
    /// this will be skipped and a warning will be logged.
    #[arg(short, long, default_value_t = usize::MAX)]
    pub max_file_size_mb: usize,

    /// The number of jobs (files) to run simultaneously
    #[arg(short, long, default_value_t = 4)]
    pub jobs: usize,

    /// File suffix for compressed files, will be stripped from input file names
    /// and added to compressed output file names
    #[arg(short, long, default_value = "")]
    pub suffix_compressed: String,

    /// Skip files with given suffix
    #[arg(short, long, default_value = "")]
    pub suffix_skip: String,

    /// Enable colorization of log output
    #[arg(short, long)]
    pub fancy_logs: bool,

    /// Use a hidden file when writing output files then rename to final name on close
    #[arg(long)]
    pub use_hidden_files: bool,

    /// Input directory events
    #[arg(
        long,
        requires = "server_mode",
        ignore_case = true,
        default_value = "both"
    )]
    pub directory_events: DirectoryListenEvents,

    /// Enables dry run mode for support of host script testing. In this mode, normal
    /// argument validation is done and parsed parameter values are logged. Any errors
    /// detected are logged normally.
    #[arg(short, long)]
    pub dry_run: bool,

    /// The command template for invoking the compression tool. Must have %{IN} and
    /// %{OUT} tokens.
    /// Example: gzip -c --best %{IN} > %{OUT}
    #[arg(env = "CAAS_COMMAND_TMPL")]
    pub command_template: String,

    /// The compression mode, ties to DB field `data_type`
    #[arg(short = 'M', long = "comp-mode", ignore_case = true)]
    pub mode: CompressionMode,

    /// Prometheus Push Exporter queue depth, when queue is full main thread blocks
    #[arg(short = 'q', long, default_value_t = 10)]
    pub exp_queue_depth: usize,

    /// Prometheus Push Gateway URL
    #[arg(short = 'u', long, default_value = "http://localhost/metrics/")]
    pub exp_url: String,

    /// Prometheus Push Exporter thread pool count
    #[arg(short = 'E', long, default_value_t = 2)]
    pub exp_threads: usize,

    /// The maximum number of database threads in the DB tokio thread pool
    #[arg(short = 'D', long, default_value_t = 3)]
    pub db_threads: u32,

    /// The maximum number of connections in the DB connection pool
    #[arg(short = 'C', long, default_value_t = 5)]
    pub db_connections: u32,

    /// Data type/file format, maps to DB field `datatype_fileformat`
    #[arg(short = 'F', long, default_value = "text/plain")]
    pub datatype_fileformat: String,

    /// Grafana label prefix
    #[arg(short = 'G', long = "grafana-label-prefix", default_value = "")]
    pub graf_label_prefix: String,

    /// Data compression algorithm to use, maps to DB field `compression_algorithm`.
    /// Example: gzip, bzip2
    #[arg(short = 'A', long, env = "COMPRESSION_ALGORITHM")]
    pub compression_algorithm: String,

    /// Server mode
    #[arg(long)]
    pub server_mode: bool,

    /// This is the main processing instance (child of the watch-dog)
    #[arg(long)]
    pub is_child: bool,

    /// Container type, typically host script name
    #[arg(long, env = "CAAS_CONTAINER_TYPE")]
    pub container_type: String,

    /// Host input directory
    #[arg(long, env = "CAAS_HOST_INPUT_DIR")]
    pub host_input_dir: String,

    /// Host output directory
    #[arg(long, env = "CAAS_HOST_OUTPUT_DIR")]
    pub host_output_dir: String,
}
