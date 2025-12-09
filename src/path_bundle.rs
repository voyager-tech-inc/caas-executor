// SPDX-FileCopyrightText: 2025 Voyager Technologies, Inc.
//
// SPDX-License-Identifier: MIT

#[derive(Debug)]
pub struct PathBundle {
    /// Path to input directory (no name)
    pub path_in: String,

    /// Path to output directory (no name)
    pub path_out: String,

    /// Path to processed directory (no name)
    pub path_proc: String,

    /// Name of input file which may or many not have a suffix depending on mode
    /// (compress/decompress) compressing or decompressing.
    pub name_in: String,

    /// Name of output file which may or many not have a suffix depending on mode
    /// (compress/decompress) compressing or decompressing.
    pub name_out: String,

    /// Full path to output file for child process, this may be a hidden file if
    /// use-hidden-files is true.
    pub fileout: String,
}

impl PathBundle {
    pub fn path_from(&self) -> String {
        format!("{}/{}", self.path_in, self.name_in)
    }

    pub fn path_to(&self) -> String {
        format!("{}/{}", self.path_out, self.name_out)
    }

    pub fn path_fileout(&self) -> &str {
        self.fileout.as_str()
    }

    pub fn is_hidden_file(&self) -> bool {
        self.fileout.contains("/.caas")
    }

    pub fn path_proc(&self) -> String {
        format!("{}/{}", self.path_proc, self.name_in)
    }
}
