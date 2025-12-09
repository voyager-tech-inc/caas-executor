// SPDX-FileCopyrightText: 2025 Voyager Technologies, Inc.
//
// SPDX-License-Identifier: MIT

use anyhow::{anyhow, Result};
use std::fs;
use std::path::Path;
use std::process::Command;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use std::time::Instant;

type CompressionMode = super::db_sqlx::CompressionMode;
type PathBundle = super::path_bundle::PathBundle;
type PushPromExporterPtr = Arc<dyn super::pm_exporter::GenericPrometheusPushExportor>; // shared_ptr
type DatabasePtr = Arc<dyn super::db_sqlx::Database>;

/// Counts the number of files that had a processing error.
static GLOBAL_ERROR_COUNT: AtomicU32 = AtomicU32::new(0);

/// Get number of files that had a processing error.
pub fn get_error_count() -> u32 {
    GLOBAL_ERROR_COUNT.load(std::sync::atomic::Ordering::Relaxed)
}

/// Increment file processing error counter.
fn inc_error_count() -> u32 {
    GLOBAL_ERROR_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

/// Context info needed for processing a single file.
pub struct FileProcessor {
    /// File suffix for compressed file
    suffix: String,

    /// All paths, and names in all combinations needed, (hopefully).
    path_bundle: PathBundle,

    /// UUID of subject file
    uuid: String,

    /// The full command line to run in child process to compress or decompress.
    command_line: String,

    /// Handle to prometheus push exportor which will send metrics to a prometheus gravel
    /// gateway.
    prom_exporter: PushPromExporterPtr,

    /// File compression mode (compress or decompress)
    mode: CompressionMode,

    /// Handle to the database module
    db: DatabasePtr,

    // Initial file size
    initial_size: f32,
}

/// Implementation for FileProcessor. One of these is created for each file in the input directory
#[rustfmt::skip]
impl FileProcessor
{
    pub fn new(
        path_bundle:    PathBundle,
        suffix:         String,
        uuid:           String,
        command_line:   String,
        mode:           CompressionMode,
        db:             DatabasePtr,
        prom_exporter:  PushPromExporterPtr) -> Self
    {
        let initial_size = get_filesize(path_bundle.path_from().as_str()) as f32;

        Self {
            path_bundle,
            suffix,
            uuid,
            command_line,
            mode,
            db,
            prom_exporter,
            initial_size,
        }
    }

    fn move_file_to_processed(&self)
    {
        let path_f = self.path_bundle.path_from();
        let path_t = self.path_bundle.path_proc();

        log::debug!("Rename {} to {}", path_f, path_t);

        if let Err(err) = std::fs::rename(&path_f, &path_t)
        {
            log::error!("Error moving file from {} to {}:{}", path_f, path_t, err);
        }
    }

    pub fn process_file(&mut self) -> Result<()>
    {
        let basename = &self.path_bundle.name_in;
        let name_no_suffix = match basename.strip_suffix(&self.suffix)
        {
            Some(name) => { name },
            None => { basename }
        };
        log::debug!("@FILE: {}: Command:{}", basename, self.command_line);

        match self.db.comp_create(self.uuid.as_str(), name_no_suffix)
        {
            Ok(_) => {},
            Err(err) =>
            {
                log::warn!("Database create failed for {name_no_suffix}:{:?}", err);
            }
        }

        let start_time = Instant::now();

        // Push an update to the database once we begin working on this file. This is
        // a non-blocking asynchronous rpc call.
        self.db.comp_processing(self.uuid.as_str())?;

        // Run the full command line for the file, blocking until done. Capture
        // stdout/sterr and exit status.
        let output = Command::new("/bin/bash")
            .args(["-c", &self.command_line])
            .output();

        let elapsed = (Instant::now() - start_time).as_secs_f32();

        // Unwrap the Result<T> to get at the Output object inside.
        let (child_out, status_ok) = match output
        {
            // This error means we were not able to get the Output because of system call
            // error, most likely a programming error.
            Err(err) =>
            {
                log::error!(
                    "@FILE: {}: Command failed:{} - cmd:{}",
                    basename, err, self.command_line);
                ("Failed for unknown reason".to_string(), false)
            }
            Ok(output) =>
            {
                if !output.status.success()
                {
                    log::error!("@FILE: {}: File failed, child", basename);
                    (crate::util::get_child_output(&output)?, false)
                }
                else
                {
                    (crate::util::get_child_output(&output)?, true)
                }
            }
        };

        // Write final database record
        if status_ok
        {
            // Compression table always store uncompressed size so pick the larger of the
            // two.  This is because this process doesn't know if it is a compress or a
            // decompress operation.
            //
            let sz1 = get_filesize(self.path_bundle.path_fileout()) as f32;
            let sz2 = get_filesize(&self.path_bundle.path_from()) as f32;
            let size = if sz1 > sz2 {
                sz1
            } else {
                sz2
            };
            self.db.comp_finish(self.uuid.as_str(), elapsed, size)?;

            // (de)compression percentage always uses size_out/size_in so compression will
            // be <= 100% and decompress will be >= 100%.
            let percent = sz1 * 100_f32 / self.initial_size;

            log::info!(
                "@FILE: {}: Ok, in:{:>1.1} MB, out:{:>1.1} MB, {:>1.2}%",
                basename,
                10e-6_f32 * self.initial_size,
                10e-6_f32 * size,
                percent,
            );
        }
        else
        {
            self.db.comp_fail(self.uuid.as_str(), elapsed, &child_out)?;
        }

        if self.path_bundle.is_hidden_file()
        {
            // Rename the file up one level from "/.caas/" sub dir.
            let path_out    = self.path_bundle.path_to();
            let path_hidden = self.path_bundle.path_fileout();
            log::info!("@FILE: {} rename hidden file {} to {}", basename, path_hidden, path_out);

            if let Err(err) = std::fs::rename(path_hidden, &path_out)
            {
                log::error!("Error moving file from {} to {}:{}", path_hidden, path_out, err);
            }

            // some compression types generate additional artifacts, such as header files
            // if existing, move them to the final output directory too
            let hidden_dir = Path::new(&path_hidden);
            let out_dir = Path::new(&path_out);

            if let Some(parent) = hidden_dir.parent()
            {
                for artifact in fs::read_dir(parent)?
                {
                    let entry = artifact?;
                    let artifact_path = entry.path();

                    if let Some(out_parent) = out_dir.parent()
                    {
                        let artifact_out = out_parent.join(entry.file_name());

                        log::info!("@FILE: {} rename hidden artifact {} to {}", basename, artifact_path.display(), artifact_out.display());

                        if let Err(err) = std::fs::rename(&artifact_path, &artifact_out)
                        {
                            log::error!("Error moving artifact from {} to {}:{}", artifact_path.display(), artifact_out.display(), err);
                        }
                    }
                }
            }
        }

        // Setup output for Prometheus.
        let ok_fail = (if status_ok { "Ok" } else { "Fail" }).to_string();
        let count = match self.mode
        {
            CompressionMode::Compress =>
            {
                super::pm_exporter::CounterId::CompressedFileCount(1, ok_fail)
            }
            CompressionMode::Decompress =>
            {
                super::pm_exporter::CounterId::DecompressedFileCount(1, ok_fail)
            }
        };

        self.notify_done(status_ok);
        if !child_out.is_empty()
        {
            log::info!("Child output:{}", child_out);
        }

        // Non-blocking call
        if let Err(err) = self.prom_exporter.increment(count)
        {
            Err(anyhow!("process_file: error sending update to prom_exporter: {:?}", err))
        }
        else
        {
            Ok(())
        }
    } // process_file

    // Notify function to be called back in file processing thread context.
    fn notify_done(&self, ok: bool)
    {
        if ok
        {
            self.move_file_to_processed();
        }
        else
        {
            // TODO: Are we supposed to move to error on failure?
            inc_error_count();
        }
    } // notify_done
} // impl FileProcessor

fn get_filesize(of_file: &str) -> u64 {
    match std::fs::metadata(of_file) {
        Ok(meta) => meta.len(),
        Err(err) => {
            log::warn!("get_filesize: {}, err:{}", of_file, err);
            0_u64
        }
    }
}
