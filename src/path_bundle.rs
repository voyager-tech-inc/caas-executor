// SPDX-FileCopyrightText: 2025 Voyager Technologies, Inc.
//
// SPDX-License-Identifier: MIT

use std::path::PathBuf;

#[derive(Debug)]
pub struct PathBundle {
    /// Path to input directory (no name)
    pub path_in: PathBuf,

    /// Path to output directory (no name)
    pub path_out: PathBuf,

    /// Path to processed directory (no name)
    pub path_proc: Option<PathBuf>,

    /// Name of input file which may or many not have a suffix depending on mode
    /// (compress/decompress) compressing or decompressing.
    pub name_in: String,

    /// Name of output file which may or many not have a suffix depending on mode
    /// (compress/decompress) compressing or decompressing.
    pub name_out: String,

    /// Full path to output file for child process, this may be a hidden file if
    /// use-hidden-files is true.
    pub fileout: PathBuf,
}

impl PathBundle {
    pub fn path_from(&self) -> PathBuf {
        self.path_in.join(self.name_in.clone())
    }

    pub fn path_to(&self) -> PathBuf {
        self.path_out.join(self.name_out.clone())
    }

    pub fn is_hidden_file(&self) -> bool {
        let path_str = self.fileout.to_string_lossy();
        path_str.contains("/.caas")
    }

    pub fn path_proc(&self) -> Option<PathBuf> {
        self.path_proc
            .as_ref()
            .map(|path_proc| path_proc.join(self.name_in.clone()))
    }
}
