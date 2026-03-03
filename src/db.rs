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

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;
    use std::time::{Duration, SystemTime};

    fn open_test_db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        init_database_conn(&conn).unwrap();
        conn
    }

    // init_database opens a file; for tests we call the setup logic directly
    fn init_database_conn(conn: &Connection) -> Result<()> {
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
        Ok(())
    }

    #[test]
    fn test_should_update_file_not_in_db() {
        // A file not yet in the DB should always need updating
        let conn = open_test_db();
        let path = std::path::Path::new("/some/new/file.txt");
        let modified = SystemTime::now();
        assert!(should_update_file(&conn, path, modified).unwrap());
    }

    #[test]
    fn test_should_update_file_unchanged() {
        // A file in the DB with the same timestamp should NOT need updating
        let conn = open_test_db();
        let path = std::path::Path::new("/some/file.txt");
        let modified = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000);
        let secs = 1_000_000i64;

        conn.execute(
            "INSERT INTO files (path, hash, size, modified) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params!["/some/file.txt", "abc123", 100i64, secs],
        )
        .unwrap();

        assert!(!should_update_file(&conn, path, modified).unwrap());
    }

    #[test]
    fn test_should_update_file_changed() {
        // A file in the DB with a different timestamp SHOULD need updating
        let conn = open_test_db();
        let path = std::path::Path::new("/some/file.txt");

        conn.execute(
            "INSERT INTO files (path, hash, size, modified) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params!["/some/file.txt", "abc123", 100i64, 1_000_000i64],
        )
        .unwrap();

        // Use a different timestamp
        let modified = SystemTime::UNIX_EPOCH + Duration::from_secs(2_000_000);
        assert!(should_update_file(&conn, path, modified).unwrap());
    }
}
