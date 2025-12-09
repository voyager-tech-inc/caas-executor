// SPDX-FileCopyrightText: 2025 Voyager Technologies, Inc.
//
// SPDX-License-Identifier: MIT

use anyhow::anyhow;
use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use sqlx::migrate::Migrator;
use sqlx::postgres::PgPoolOptions;
use sqlx::{Pool, Postgres};
use std::path::Path;
use std::sync::Arc;
use std::{thread, time};

/// The Generic Database trait for writing to Compression table, this trait needs to impl
/// Send/Sync to be able to be wrapped in an Arc
///
pub trait Database: Send + Sync {
    fn wait_for_container_record(&self) -> anyhow::Result<()>;
    fn comp_create(&self, uuid: &str, file: &str) -> anyhow::Result<()>;
    fn comp_finish(&self, uuid: &str, proc_time: f32, final_size: f32) -> anyhow::Result<()>;
    fn comp_fail(&self, uuid: &str, proc_time: f32, error_msg: &str) -> anyhow::Result<()>;
    fn comp_processing(&self, uuid: &str) -> anyhow::Result<()>;
}

/// Compression state, should match DB enum type `comp_state`
#[derive(Clone, Debug, PartialEq, PartialOrd, sqlx::Type, Deserialize, Serialize)]
#[sqlx(type_name = "comp_state")]
/// State of file transfer
pub enum CompressionState {
    /// Indicates file is waiting to be processed.
    Pending,

    /// Indicates file is currently being processed (compressed or decompressed).
    Processing,

    /// Indicates file processing failed, error field will contain failure reason.
    Failed,

    /// Indicates file processing has completed successfully.
    Done,
}

#[derive(Clone, Debug, PartialEq, PartialOrd, sqlx::Type, Deserialize, Serialize, ValueEnum)]
#[sqlx(type_name = "comp_mode")]
/// Compression mode, should match DB enum type "comp_mode".
pub enum CompressionMode {
    /// File is being compressed.
    Compress,

    /// File is being decompressed.
    Decompress,
}

macro_rules! map_result {
    ($from: expr) => {{
        match $from {
            Ok(__) => anyhow::Result::Ok(()),
            Err(e) => anyhow::Result::Err(anyhow!("{}", e)),
        }
    }};
}

///SqlxDatabase handle
#[derive(Debug)]
pub struct SqlxDatabase {
    pool: Pool<Postgres>,
    runtime: tokio::runtime::Runtime,
    config_hash: String,
}

// Private impl
impl SqlxDatabase {
    /// Creates a new DB handle
    ///
    /// # Arguments
    /// * connections   - The maximum number of connections in the DB connection pool
    /// * thread_count  - Number `tokio` threads to start in the `tokio` runtime
    /// * config_hash   - Unique primary key e.g., input directory and command template
    /// # Example:
    /// ```
    /// let db = SqlxDatabase::new(3, 5, "Gzip", "text/html")?;
    /// ```
    pub fn new(connections: u32, thread_count: u32, config_hash: &str) -> anyhow::Result<Self> {
        let db_url = crate::util::get_db_url();

        log::debug!(
            "Database::new url={db_url}, connections={connections}, number of tokio threads={thread_count}"
        );

        // Ordinarily the tokio runtime (the tokio scheduler instance) is created at the
        // top of main and the whole program is async but caas-ftm::main was not an async
        // (tokio) program when this module was added but SQLx is async. So we keep the
        // tokio runtime inside this class.  I think this makes this class a Singleton now
        // since we can only have one runtime.  If main becomes async then this will have
        // to be refactored to use the runtime created there.
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(thread_count as usize)
            .enable_io()
            .enable_time()
            .build()?;

        // This method is being called from a synchronous (non tokio task) and we need to
        // run a tokio task so we need to spawn a new tokio task on the runtime and block
        // here until it completes.
        //
        let future = runtime.block_on(Self::async_get_connection_pool(&db_url, connections));
        let pool = match future {
            Ok(pool) => pool,
            Err(err) => {
                return Err(anyhow!("{}", err));
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

        Ok(Self {
            pool,
            runtime,
            config_hash: config_hash.to_owned(),
        })
    }

    /// Creates the Postgresql connection pool. The pool will start out with 1 connection
    /// created now (to test the connection URL), and it will add new connections when
    /// needed but not more than `count` connections.
    ///
    ///  # Arguments
    ///
    ///  * url   - Database connection URL
    ///  * count - maximum number of open connections before requests are queued
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

    async fn async_get_container_record(&self) -> String {
        match sqlx::query!(
            "SELECT config_hash from containers WHERE config_hash = $1",
            self.config_hash
        )
        .fetch_one(&self.pool)
        .await
        {
            Ok(record) => record.config_hash,
            Err(_) => String::new(),
        }
    }

    // Private (non public) async function
    async fn async_comp_create(&self, uuid: &str, filename: &str) -> anyhow::Result<()> {
        log::debug!("comp_create: DB_INSERT uuid={uuid}, file={filename}");

        let now = chrono::Utc::now().naive_utc();
        let zero = 0_f32;
        let state = CompressionState::Pending;

        map_result!(
            sqlx::query!(
                "INSERT INTO compression (\
                 id, timestamp, filename, filesize, state, processing_time, config_hash) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7)",
                /* $1 */ uuid,
                /* $2 */ now,
                /* $3 */ filename,
                /* $4 */ zero,
                /* $5 */ state as CompressionState,
                /* $6 */ zero,
                /* $7 */ self.config_hash,
            )
            .execute(&self.pool)
            .await
        )
    }

    async fn async_db_finish(
        &self,
        uuid: &str,
        proc_time: f32,
        final_size: f32,
    ) -> anyhow::Result<()> {
        log::debug!(
            "async_db_finish: uuid={}, proc_time={}, final_size: {}",
            uuid,
            proc_time,
            final_size
        );

        let now = chrono::Utc::now().naive_utc();
        let state = CompressionState::Done;

        map_result!(
            sqlx::query!(
                "UPDATE compression SET timestamp = $2, state = $3, filesize = $4, \
                 processing_time = $5 WHERE id = $1",
                uuid,
                now,
                state as CompressionState,
                final_size,
                proc_time,
            )
            .execute(&self.pool)
            .await
        )
    }

    async fn async_db_fail(
        &self,
        uuid: &str,
        proc_time: f32,
        error_msg: &str,
    ) -> anyhow::Result<()> {
        log::debug!("async_db_fail: uuid={uuid}");

        let now = chrono::Utc::now().naive_utc();
        let state = CompressionState::Failed;

        map_result!(
            sqlx::query!(
                "UPDATE compression SET timestamp = $2, state = $3, processing_time = $4, \
                 error = $5 WHERE id = $1",
                uuid,
                now,
                state as CompressionState,
                proc_time,
                error_msg,
            )
            .execute(&self.pool)
            .await
        )
    }

    async fn async_db_processing(&self, uuid: &str) -> anyhow::Result<()> {
        log::debug!("async_db_processing: uuid={uuid}");

        let now = chrono::Utc::now().naive_utc();
        let state = CompressionState::Processing;

        map_result!(
            sqlx::query!(
                "UPDATE compression SET timestamp = $2, state = $3 WHERE id = $1",
                uuid,
                now,
                state as CompressionState,
            )
            .execute(&self.pool)
            .await
        )
    }
}

// Implement the public Database trait
impl Database for SqlxDatabase {
    fn wait_for_container_record(&self) -> anyhow::Result<()> {
        let fname = "wait_for_container_record";
        let jiffy = time::Duration::from_millis(100);

        for _n in 1..100 {
            thread::sleep(jiffy);

            if self.runtime.block_on(self.async_get_container_record()) == self.config_hash {
                log::debug!("{fname}: Ok");
                return Ok(());
            };
            log::warn!("{fname}: still waiting...");
            continue;
        }
        Err(anyhow!(
            "Timed out waiting for container:{}",
            self.config_hash
        ))
    }

    fn comp_create(&self, uuid: &str, file: &str) -> anyhow::Result<()> {
        let fname = "comp_create";

        // Call the asynchronous database insertion method using self.runtime.
        match self.runtime.block_on(self.async_comp_create(uuid, file)) {
            Ok(_) => {
                log::debug!("{fname}: uuid:{uuid}, file:{file} db write Ok");
                anyhow::Result::Ok(())
            }
            Err(e) => {
                log::warn!("{fname}: uuid:{uuid}, file:{file} db write failed:{e}");
                anyhow::Result::Err(anyhow!("{}", e))
            }
        }
    }

    fn comp_finish(&self, uuid: &str, proc_time: f32, final_size: f32) -> anyhow::Result<()> {
        let fname = "comp_finish";

        let status = match self
            .runtime
            .block_on(self.async_db_finish(uuid, proc_time, final_size))
        {
            Ok(_) => {
                log::debug!("{fname}: uuid:{uuid} db write Ok");
                anyhow::Result::Ok(())
            }
            Err(e) => {
                log::warn!("{fname}: uuid:{uuid} db write failed:{e}");
                anyhow::Result::Err(anyhow!("{}", e))
            }
        };
        status
    }

    fn comp_fail(&self, uuid: &str, proc_time: f32, error_msg: &str) -> anyhow::Result<()> {
        let fname = "comp_fail";

        match self
            .runtime
            .block_on(self.async_db_fail(uuid, proc_time, error_msg))
        {
            Ok(_) => {
                log::debug!("{fname}: uuid:{uuid} err:{error_msg} db write Ok");
                anyhow::Result::Ok(())
            }
            Err(e) => {
                log::warn!("{fname}: uuid:{uuid} db write failed:{e}");
                anyhow::Result::Err(anyhow!("{}", e))
            }
        }
    }

    fn comp_processing(&self, uuid: &str) -> anyhow::Result<()> {
        let fname = "comp_processing";

        match self.runtime.block_on(self.async_db_processing(uuid)) {
            Ok(_) => {
                log::debug!("{fname}: uuid:{uuid} db write Ok");
                anyhow::Result::Ok(())
            }
            Err(e) => {
                log::warn!("{fname}: uuid:{uuid} db write failed:{e}");
                anyhow::Result::Err(anyhow!("{}", e))
            }
        }
    }
}

//impl Send for dyn Database{}

/// Creates a new DB handle
///
pub fn create_database(
    connections: u32,
    thread_count: u32,
    config_hash: &str,
) -> anyhow::Result<Arc<dyn Database>> {
    // First attempt to create a SqlxDatabase, if that fails create a Null Database
    match SqlxDatabase::new(connections, thread_count, config_hash) {
        Ok(db) => Ok(Arc::new(db)),
        Err(err) => {
            log::info!("Failed to connect to database:{err}\n  Using null database instead");
            Ok(Arc::new(NullDatabase {}))
        }
    }
}

struct NullDatabase {}
impl Database for NullDatabase {
    fn wait_for_container_record(&self) -> anyhow::Result<()> {
        Ok(())
    }

    fn comp_create(&self, _uuid: &str, _file: &str) -> anyhow::Result<()> {
        Ok(())
    }

    fn comp_finish(&self, _uuid: &str, _time: f32, _size: f32) -> anyhow::Result<()> {
        Ok(())
    }

    fn comp_fail(&self, _uuid: &str, _time: f32, _error_msg: &str) -> anyhow::Result<()> {
        Ok(())
    }

    fn comp_processing(&self, _uuid: &str) -> anyhow::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
pub mod tests {
    // Import names from outer scope.
    use super::*;

    // Row struct maps to the database schema for this table.  Used in the tests below to
    // verify changes to the DB where correct.
    #[derive(Debug, sqlx::FromRow, Deserialize)]
    #[allow(non_snake_case)]
    struct CompressionRow {
        id: String,
        timestamp: sqlx::types::chrono::NaiveDateTime,
        filename: String,
        filesize: f32,
        state: CompressionState,
        processing_time: f32,
        config_hash: String,
        error: Option<String>, // This field may be NULL so this is an Optional<>
    }

    // Deletes a record from the compression table given a primary key, return number of rows
    // affected, which should be 0 if row not found or 1 if it was.
    async fn delete_compression_record(db: &SqlxDatabase, uuid: &str) -> sqlx::Result<u64> {
        let rows_affected = sqlx::query!(r#"DELETE FROM compression WHERE id = $1"#, uuid)
            .execute(&db.pool)
            .await?
            .rows_affected();
        Ok(rows_affected)
    }

    // Compression table has a foreign column 'config_hash' that is tied to a column of
    // same name in containers table. A row with that same config_hash value must exist in
    // containers in both tables in order for the insert to succeed.  This function adds a
    // test record and is called before insertion into compression for each of the tests.
    async fn add_container_record(
        db: &SqlxDatabase,
        config_hash: &str,
        command_template: &str,
        container_id: &str,
        delete_first: bool,
        container_name: &str,
    ) -> u64 {
        if delete_first {
            let _unused_result = sqlx::query!(
                "DELETE FROM containers WHERE config_hash = $1;",
                config_hash
            )
            .execute(&db.pool)
            .await;
        }

        sqlx::query!(
            r#"
            INSERT INTO containers
            (config_hash,
             container_id,
             command_template,
             mode,
             datatype_fileformat,
             compression_algorithm,
             is_server,
             start_time,
             container_args,
             input_directory,
             output_directory,
             container_type,
             grafana_label)
            VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
            ON CONFLICT (config_hash)
            DO UPDATE SET
              container_id = $2,
              mode = 'Compress',
              datatype_fileformat = $5,
              compression_algorithm = $6,
              is_server = $7,
              start_time = $8,
              container_args = $9,
              output_directory = $10,
              grafana_label = $13;
            "#,
            config_hash, // $1
            container_id,
            "container_type",
            CompressionMode::Compress as CompressionMode,
            "video/mp4".to_owned(),
            "lzma".to_owned(),
            false,
            chrono::Utc::now().naive_utc(),
            "container_args a b c".to_owned(),
            "/directory/input".to_owned(),
            "/directory/output".to_owned(),
            container_name,
            command_template
        )
        .execute(&db.pool)
        .await
        .unwrap()
        .rows_affected()
    }

    // Query compression table, returns a Vector of rows found, given a uuid which is
    // primary key, it should return 0 or 1 rows.
    async fn get_all_rows_for_uuid(
        db: &SqlxDatabase,
        uuid: &str,
    ) -> sqlx::Result<Vec<CompressionRow>> {
        // NOTE: Can't use 'select *' because of the enum field. Also note how the enum
        // CompressionState is handled.  This is the magic sauce you need to map the
        // postgresql enum to a strongly typed rust enum.
        let rows = sqlx::query_as!(
            CompressionRow,
            r#"
    SELECT
      id,
      timestamp,
      filename,
      filesize,
      state as "state: _",
      processing_time,
      error,
      config_hash
      FROM compression
      WHERE id = $1"#,
            uuid
        )
        .fetch_all(&db.pool)
        .await?;
        Ok(rows)
    }

    // Tests run concurrently so must use unique keys for each one. Using key similar to
    // the test method name.

    #[test]
    fn comp_create() -> anyhow::Result<()> {
        let config_hash = "config-comp-create";
        let uuid = "uuid-compression-create";
        let file = "file-compression-create";
        let db = SqlxDatabase::new(3, 5, config_hash)?;

        let n = db.runtime.block_on(delete_compression_record(&db, uuid))?;
        assert_eq!(n, 0_u64);

        let n = db.runtime.block_on(add_container_record(
            &db,
            config_hash,
            "cmd",
            "ctnr-1",
            true,
            "container-1",
        ));
        assert_eq!(n, 1_u64);

        // Test creation of a record in Compression table with a given uuid/filename to be
        // verified below.
        db.comp_create(uuid, file)?;

        // Read the record back and verify the expected row values.
        let rows = db.runtime.block_on(get_all_rows_for_uuid(&db, uuid))?;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, uuid);
        assert_eq!(rows[0].filename, file);
        assert_eq!(rows[0].filesize, 0f32);
        assert_eq!(rows[0].state, CompressionState::Pending);
        assert_eq!(rows[0].processing_time, 0_f32);
        assert_eq!(rows[0].filesize, 0_f32);
        assert_eq!(rows[0].error, None);
        assert_eq!(
            rows[0].timestamp.date(),
            chrono::Utc::now().naive_utc().date()
        );
        assert_eq!(rows[0].config_hash, config_hash);

        let rows = db.runtime.block_on(delete_compression_record(&db, uuid))?;
        assert_eq!(rows, 1_u64);
        Ok(())
    }

    #[test]
    fn comp_finish() -> anyhow::Result<()> {
        let config_hash = "config-compression-finish";
        let uuid = "uuid-compression-finish";
        let file = "file-comp-finish";
        let db = SqlxDatabase::new(3, 5, config_hash)?;
        let _ = db.runtime.block_on(delete_compression_record(&db, uuid));
        let _ = db.runtime.block_on(add_container_record(
            &db,
            config_hash,
            "cmd",
            "ctnr-1",
            true,
            "container-1",
        ));

        // Test API 'comp_finish'
        db.comp_create(uuid, file)?;
        db.comp_finish(uuid, 10.125, 123456.125)?;

        let rows = db.runtime.block_on(get_all_rows_for_uuid(&db, uuid))?;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, uuid);
        assert_eq!(rows[0].filename, file);
        assert_eq!(rows[0].state, CompressionState::Done);
        assert_eq!(rows[0].processing_time, 10.125_f32);
        assert_eq!(rows[0].filesize, 123456.125_f32);
        assert_eq!(rows[0].error, None);
        assert_eq!(
            rows[0].timestamp.date(),
            chrono::Utc::now().naive_utc().date()
        );
        assert_eq!(rows[0].config_hash, config_hash);

        let rows = db.runtime.block_on(delete_compression_record(&db, uuid))?;
        assert_eq!(rows, 1_u64);
        Ok(())
    }

    #[test]
    fn comp_fail() -> anyhow::Result<()> {
        let config_hash = "config-compression-fail";
        let uuid = "uuid-compression-fail";
        let file = "file-comp-fail";
        let db = SqlxDatabase::new(3, 5, config_hash)?;
        let _ = db.runtime.block_on(delete_compression_record(&db, uuid));
        let _ = db.runtime.block_on(add_container_record(
            &db,
            config_hash,
            "cmd",
            "ctnr-1",
            true,
            "container-1",
        ));

        // Test API 'comp_fail'
        db.comp_create(uuid, file)?;
        db.comp_fail(uuid, 123.1, "Got an error")?;

        let rows = db.runtime.block_on(get_all_rows_for_uuid(&db, uuid))?;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, uuid);
        assert_eq!(rows[0].filename, file);
        assert_eq!(rows[0].filesize, 0_f32);
        assert_eq!(rows[0].state, CompressionState::Failed);
        assert_eq!(rows[0].processing_time, 123.1f32);
        assert_eq!(rows[0].error, Some("Got an error".to_string()));
        assert_eq!(
            rows[0].timestamp.date(),
            chrono::Utc::now().naive_utc().date()
        );
        assert_eq!(rows[0].config_hash, config_hash);

        let rows = db.runtime.block_on(delete_compression_record(&db, uuid))?;
        assert_eq!(rows, 1_u64);
        Ok(())
    }

    #[test]
    fn comp_processing() -> anyhow::Result<()> {
        let config_hash = "config-compression-processing";
        let uuid = "uuid-compression-processing";
        let file = "file-comp-processing";
        let db = SqlxDatabase::new(3, 5, config_hash)?;

        let _ = db.runtime.block_on(delete_compression_record(&db, uuid));
        let _ = db.runtime.block_on(add_container_record(
            &db,
            config_hash,
            "cmd",
            "ctnr-1",
            true,
            "container-1",
        ));

        // Test API 'comp_update'
        db.comp_create(uuid, file)?;
        db.comp_processing(uuid)?;

        let rows = db.runtime.block_on(get_all_rows_for_uuid(&db, uuid))?;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, uuid);
        assert_eq!(rows[0].filename, file);
        assert_eq!(rows[0].filesize, 0f32);
        assert_eq!(rows[0].state, CompressionState::Processing);
        assert_eq!(rows[0].processing_time, 0.0f32);
        assert_eq!(rows[0].error, None);
        assert_eq!(
            rows[0].timestamp.date(),
            chrono::Utc::now().naive_utc().date()
        );
        assert_eq!(rows[0].config_hash, config_hash);

        let rows = db.runtime.block_on(delete_compression_record(&db, uuid))?;
        assert_eq!(rows, 1_u64);
        Ok(())
    }

    #[test]
    fn null_database() -> anyhow::Result<()> {
        let uuid = "uuid-create-database";
        let file = "file-create-database";

        let db = NullDatabase {};

        db.comp_create(uuid, file)?;
        db.comp_processing(uuid)?;
        Ok(())
    }
}
