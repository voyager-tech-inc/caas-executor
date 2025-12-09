-- SPDX-FileCopyrightText: 2025 Voyager Technologies, Inc.
--
-- SPDX-License-Identifier: MIT

CREATE TYPE comp_state AS ENUM ('Pending', 'Processing', 'Failed', 'Done');
CREATE TYPE comp_mode  AS ENUM ('Compress', 'Decompress');

CREATE TABLE containers (
  config_hash           TEXT        NOT NULL PRIMARY KEY, -- Hash of command_template & input directory
  command_template      TEXT        NOT NULL, -- Raw unhashed command template
  container_id          TEXT,
  container_type        TEXT        NOT NULL,
  mode                  COMP_MODE   NOT NULL,
  datatype_fileformat   TEXT        NOT NULL, -- The type of data being compressed: e.g.: video/mp4
  compression_algorithm TEXT        NOT NULL, -- Compression algorithm: e.g.: gzip, lz4, lzma
  is_server             BOOL        NOT NULL,
  start_time            TIMESTAMP   NOT NULL, -- Container start time.
  stop_time             TIMESTAMP,            -- Container stop time, NULL if container is running
  container_args        TEXT        NOT NULL, -- Redundant with config_hash but is readable
  input_directory       TEXT        NOT NULL,
  output_directory      TEXT        NOT NULL,
  grafana_label         TEXT        NOT NULL
);

COMMENT ON COLUMN containers.config_hash      IS 'Unique primary key e.g., input directory and command template';
COMMENT ON COLUMN containers.command_template IS 'Raw unhashed command template';
COMMENT ON COLUMN containers.container_id     IS 'Dockerd assigned ID, will be NULL when container is not running';
COMMENT ON COLUMN containers.is_server        IS 'Indicates running in server mode, else one shot mode';
COMMENT ON COLUMN containers.start_time       IS 'The container start time';
COMMENT ON COLUMN containers.stop_time        IS 'The container exit time, null if still running';
COMMENT ON COLUMN containers.container_args   IS 'Container full command line';
COMMENT ON COLUMN containers.input_directory  IS 'Host input directory';
COMMENT ON COLUMN containers.output_directory IS 'Host output directory';
COMMENT ON COLUMN containers.grafana_label    IS 'Container name to display on UI';
COMMENT ON COLUMN containers.container_type   IS 'Host script name without .py suffix';
COMMENT ON COLUMN containers.mode             IS 'A value of COMP_MODE enum';
COMMENT ON COLUMN containers.datatype_fileformat IS 'The type of data being compressed: e.g.: video/mp4';
COMMENT ON COLUMN containers.compression_algorithm IS 'Compression algorithm: e.g.: gzip, lz4, lzma';

CREATE TABLE compression (
  id                 TEXT        NOT NULL PRIMARY KEY, -- uuid
  timestamp          TIMESTAMP   NOT NULL,
  filename           TEXT        NOT NULL,
  filesize           REAL        NOT NULL,
  state              COMP_STATE  NOT NULL,
  processing_time    REAL        NOT NULL,
  error              TEXT,
  config_hash        TEXT NOT NULL REFERENCES containers
);
CREATE INDEX compression_index ON compression (timestamp, filename) WITH (deduplicate_items  = off);

ALTER TABLE compression ADD CONSTRAINT ck_proctime CHECK (processing_time >= 0::real);
ALTER TABLE compression ADD CONSTRAINT ck_name     CHECK (length(filename) <= 2048 AND length(filename) > 0);
ALTER TABLE compression ADD CONSTRAINT ck_filesize CHECK (filesize >= 0::real);
ALTER TABLE containers  ADD CONSTRAINT ck_stop_tm  CHECK (stop_time is NULL OR stop_time >= start_time);

ALTER TABLE containers ADD CONSTRAINT ck_cmpalgo
    CHECK (length(compression_algorithm) <= 128 AND length(compression_algorithm) > 0);

ALTER TABLE containers ADD CONSTRAINT ck_dtformat
    CHECK (length(datatype_fileformat) <= 128 AND length(datatype_fileformat) > 0);

COMMENT ON COLUMN compression.id        is 'Unique identifier for the file';
COMMENT ON COLUMN compression.timestamp is '(De)compress starting time, GMT (UTC) time';
COMMENT ON COLUMN compression.filename  is 'File name only, no path';
COMMENT ON COLUMN compression.filesize  is 'Files size in bytes, if state is Decompressed, then \
this is uncompressed size else it is compressed size';
COMMENT ON COLUMN compression.state     is 'A value of COMP_STATE enum';
COMMENT ON COLUMN compression.processing_time is 'Total time to (De)compress file in seconds';
COMMENT ON COLUMN compression.error     is 'Error message if state is Failed';

CREATE OR REPLACE VIEW compress_full_view AS
    SELECT timestamp, filename, filesize, state, processing_time,
       compression.config_hash, mode, compression_algorithm,
       datatype_fileformat, container_type FROM compression
    INNER JOIN containers ON containers.config_hash = compression.config_hash;
