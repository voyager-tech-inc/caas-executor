// SPDX-FileCopyrightText: 2025 Voyager Technologies, Inc.
//
// SPDX-License-Identifier: MIT

use anyhow::{anyhow, Result};
use sha1_smol::Sha1; //  Digest}
use std::env;
use std::ffi::OsStr;
use std::path::Path;
use walkdir::{DirEntry, WalkDir};

// Filter predicate, returns true if for a hidden file, i.e., its name starts with '.'
//
fn is_hidden(entry: &DirEntry) -> bool {
    entry
        .file_name()
        .to_str()
        .map(|s| s.starts_with('.'))
        .unwrap_or(false)
}

// Convert an OS String to normal String
//
fn to_string(name: &std::ffi::OsStr) -> String {
    name.to_str().unwrap_or("").to_string()
}

/// Gets a list of files from skipping some files:
/// ```
///  SKIPPED ARE:
///    file_size >= self.max_size_mb
///    or NAME matches CONFIG file name
///    and skipping directories
/// ```
pub fn get_file_list(
    dir: &str,
    max_size_mb: usize,
    config_skip: &str,
    suffix_skip: &str,
) -> Result<Vec<String>, std::io::Error> {
    //
    let mut result = Vec::new();
    let max_size_mb = max_size_mb as f32; // convert arg to float

    // Get an iterator over all files in dir which: 1) filters out hidden files, 2) does
    // not descend into sub-directories and 3) does not cross file system broundries so it
    // will stay within the bind mount point, which probably is not need given
    // max_depth=1.
    let walker = WalkDir::new(dir)
        .max_depth(1)
        .sort_by_file_name()
        .same_file_system(true)
        .into_iter()
        .filter_entry(|e| !is_hidden(e));

    for entry in walker {
        match entry {
            Ok(e) => {
                if let Some(name) = is_valid_file(e.path(), max_size_mb, config_skip, suffix_skip) {
                    result.push(name);
                }
            }
            Err(err) => {
                let path = err.path().unwrap_or(Path::new("")).display();

                if let Some(inner) = err.io_error() {
                    log::error!("get_file_list:: {} {}: {}", dir, path, inner);
                } else {
                    log::error!("get_file_list:: unknown error occurred: {}", path);
                }
            }
        }
    }
    Ok(result)
}

pub fn is_valid_file(
    fullpath: &std::path::Path,
    max_size_mb: f32,
    config_skip: &str,
    suffix_skip: &str,
) -> Option<String> {
    let name = match fullpath.file_name() {
        Some(nm) => to_string(nm),
        None => {
            log::error!("Got path name that is not value UTF8:{:?}", fullpath);
            return None;
        }
    };

    if fullpath.extension().and_then(OsStr::to_str) == Some(suffix_skip.trim_start_matches('.')) {
        log::info!("@FILE: {}: Skipped file with suffix {}", name, suffix_skip);

        #[cfg(test)]
        {
            println!("@FILE: {}: Skipped file with suffix {}", name, suffix_skip);
        }
        return None;
    }
    if name.starts_with(".syncthing.") {
        log::info!("@FILE: {}: Skipped file with prefix .syncthing.", name);

        #[cfg(test)]
        {
            println!("@FILE: {}: Skipped file with prefix .syncthing.", name);
        }
        return None;
    }

    let metadata = match fullpath.symlink_metadata() {
        Ok(md) => md,
        Err(err) => {
            log::error!("Error getting metadata for {:?}, {:?}", name, err);
            return None;
        }
    };

    if !metadata.file_type().is_file() {
        log::info!("@FILE: {}: Skipped non file", name);
        return None;
    }
    if metadata.file_type().is_symlink() {
        log::info!("@FILE: {}: Skipped symlink file", name);
        #[cfg(test)]
        {
            println!("@FILE: {}: Skipped symlink file", name);
        }
        return None;
    }
    if name == config_skip {
        log::info!("@FILE: {}: Skipped config file", name);

        #[cfg(test)]
        {
            println!("@FILE: {}: Skipped config file", name);
        }
        return None;
    }
    let size_mb = metadata.len() as f32 * 1e-6;
    #[cfg(test)]
    {
        println!("@FILE: name={name}, size_mb={size_mb} max_size_mb={max_size_mb}");
    }

    if size_mb > max_size_mb {
        log::warn!(
            "@FILE: {}: Large file skipped, {:1.3} MB > {:2.3} MB",
            name,
            size_mb,
            max_size_mb
        );
        return None;
    }
    log::debug!("Size {:<10} MB : {}", size_mb, name);
    Some(name)
}

pub fn get_db_url() -> String {
    let fallback_url = "postgresql://postgres@localhost";
    match std::env::var("DATABASE_URL") {
        Ok(val) => {
            log::info!("DATABASE_URL found in env:{val}");
            val
        }
        Err(_) => {
            log::info!("DATABASE_URL not set, using default url:{fallback_url}");
            fallback_url.to_string()
        }
    }
}

pub fn get_container_name_env() -> Option<String> {
    for (key, value) in env::vars() {
        println!("{}: {}", key, value);
    }

    match env::var("CONTAINER_NAME") {
        Ok(val) if !val.is_empty() => {
            log::info!("CONTAINER_NAME found in env: {}", val);
            Some(val) // Return Some(val) if it's not empty
        }
        _ => {
            log::info!("CONTAINER_NAME not set or is empty, using default value");
            None // Return None if it's not set or empty
        }
    }
}

/// Compute config_hash using command_template from CLI and the host input directory.
pub fn get_config_hash(host_input_dir: &str, command_template: &str) -> Result<String> {
    let mut config_hash = command_template.to_string();

    // Remove all whitespace from command template.
    config_hash.retain(|c| !c.is_whitespace());

    // Append input directory...
    let config_hash = format!("{config_hash}{host_input_dir}");
    let sha1 = Sha1::from(config_hash.trim().as_bytes());
    let result = sha1.hexdigest();

    log::trace!("get_config_hash: Using string \"{config_hash}\"");
    log::trace!("get_config_hash: The result is {result}");

    Ok(result)
}

/// Get all output from a child process and return it as a String.
pub fn get_child_output(output: &std::process::Output) -> Result<String> {
    let mut result = String::new();
    result.push_str(std::str::from_utf8(&output.stderr)?);
    result.push_str(std::str::from_utf8(&output.stdout)?);

    Ok(result)
}

pub fn get_env(key: &str) -> Result<String> {
    match std::env::var(key) {
        Ok(value) => {
            log::debug!("util::get_env {key} = {value}");
            println!("util::get_env {key} = {value}"); // for unit testing
            Ok(value)
        }
        Err(err) => Err(anyhow!("Lookup {key} : {err}")),
    }
}

#[cfg(test)]
mod tests {
    // Import names from outer scope.
    use super::*;
    use std::fs;
    //use std::process::{Child, Command, Stdio};

    #[test]
    fn config_hash() -> Result<()> {
        // With extra whitespace and an embedded newline
        let cli_1 = "caas-gzip --fast   --input test/input   --output test/output %{IN} %{OUT} 
 ";
        // Canonical form of above would look like this.
        let cli_2 = "caas-gzip --fast --input test/input --output test/output %{IN} %{OUT}";

        // Generate the hash from cli_[12]
        let hash_1 = get_config_hash("/tmp/foobar", cli_1)?;
        let hash_2 = get_config_hash("/tmp/foobar", cli_2)?;

        // verify they match
        assert_eq!(hash_1, hash_2);

        // Prints get discarded unless you add --show-output to test command line
        println!("Hash_1 is {hash_1}");
        Ok(())
    }

    fn purge_tree(dir: &str) {
        std::process::Command::new("rm")
            .args(["-rf", dir])
            .output()
            .expect("Failed to execute process");
    }

    #[test]
    fn walk_dir_test() -> Result<()> {
        let base_dir = "/tmp/walkdir_test".to_string();
        let file_1 = "file_1";
        let file_2 = "file_2";
        let file_3 = "file_3";

        purge_tree(base_dir.as_str());

        fs::create_dir_all(format!("{base_dir}/subdir"))?;

        // Create a test tree to walk
        let cmd = format!(
            "touch \
                           {base_dir}/{file_1} \
                           {base_dir}/{file_2} \
                           {base_dir}/skip_file \
                           {base_dir}/subdir/subdir_file; \
                           ln -s /bin/bash {base_dir}/bash"
        );

        std::process::Command::new("/bin/bash")
            .arg("-c")
            .arg(cmd)
            .output()
            .expect("Failed to execute process");

        // Create a 10 MB file that will be accepted
        std::process::Command::new("/bin/dd")
            .arg("bs=1048576")
            .arg("count=10")
            .arg("if=/dev/zero")
            .arg(format!("of={base_dir}/{file_3}"))
            .output()
            .expect("Failed to execute process");

        // Create a 20 MB file that will be rejected
        std::process::Command::new("/bin/dd")
            .arg("bs=1048576")
            .arg("count=20")
            .arg("if=/dev/zero")
            .arg(format!("of={base_dir}/file_4"))
            .output()
            .expect("Failed to execute process");

        let file_list = get_file_list(&base_dir, 15, "skip_file", "")?;

        println!("\nGot {} files", file_list.len());
        for file in file_list.iter() {
            println!("  FILE:: {file}");
        }

        // Expecting 3 files, file_1, file_2, file_3
        assert_eq!(3, file_list.len());
        assert_eq!(file_1, file_list[0]);
        assert_eq!(file_2, file_list[1]);
        assert_eq!(file_3, file_list[2]);

        purge_tree(base_dir.as_str());

        Ok(())
    }
}
