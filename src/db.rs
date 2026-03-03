use crate::fs::path_to_str;
use anyhow::Result;
use rusqlite::{params, Connection};
use std::path::Path;
use std::time::SystemTime;

pub fn init_database(path: &Path) -> Result<Connection> {
    let conn = Connection::open(path)?;

    conn.execute(
        "CREATE TABLE IF NOT EXISTS files (
            path TEXT PRIMARY KEY,
            hash TEXT NOT NULL,
            size INTEGER NOT NULL,
            modified INTEGER NOT NULL
        )",
        [],
    )?;

    conn.execute(
        "CREATE TABLE IF NOT EXISTS directories (
            path TEXT PRIMARY KEY,
            hash TEXT NOT NULL,
            size INTEGER NOT NULL
        )",
        [],
    )?;

    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_file_hash ON files(hash)",
        [],
    )?;

    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_dir_hash ON directories(hash)",
        [],
    )?;

    Ok(conn)
}

pub fn should_update_file(conn: &Connection, path: &Path, modified: SystemTime) -> Result<bool> {
    let path_str = path_to_str(path)?;

    let mut stmt = conn.prepare("SELECT modified FROM files WHERE path = ?1")?;

    let result: Result<i64, rusqlite::Error> = stmt.query_row(params![path_str], |row| row.get(0));

    match result {
        Ok(stored_modified) => {
            let modified_secs = modified.duration_since(SystemTime::UNIX_EPOCH)?.as_secs() as i64;
            Ok(modified_secs != stored_modified)
        }
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(true),
        Err(e) => Err(e.into()),
    }
}
