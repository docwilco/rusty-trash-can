use crate::Error;
use log::info;
use rusqlite::Connection;

const VERSION_SCRIPTS: [(&str, &str); 2] = [
    ("schema v1", include_str!("schema_v1.sql")),
    ("schema v2", include_str!("schema_v2.sql")),
];

pub fn check_and_upgrade(conn: &mut Connection) -> Result<(), Error> {
    // Get user_version pragma
    let user_version: usize = conn.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    if user_version == VERSION_SCRIPTS.len() {
        // No need to upgrade
        return Ok(());
    }
    let tx = conn.transaction()?;
    // Execute scripts, skip those already executed according to pragma
    for (name, script) in &VERSION_SCRIPTS[user_version..] {
        info!("Executing {}", name);
        tx.execute_batch(script)?;
    }
    // Update pragma
    tx.execute(
        &format!("PRAGMA user_version = {}", VERSION_SCRIPTS.len()),
        [],
    )?;
    tx.commit()?;
    info!("Schema updated to version {}", VERSION_SCRIPTS.len());
    Ok(())
}
