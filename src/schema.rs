use crate::Error;
use rusqlite::Connection;

const VERSION_SCRIPTS: [&str; 2] = [include_str!("schema_v1.sql"), include_str!("schema_v2.sql")];

pub fn check_and_upgrade_schema(conn: &mut Connection) -> Result<(), Error> {
    // Get user_version pragma
    let user_version: usize = conn.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    if user_version == VERSION_SCRIPTS.len() {
        // No need to upgrade
        return Ok(());
    }
    let tx = conn.transaction()?;
    // Execute scripts, skip those already executed according to pragma
    for script in &VERSION_SCRIPTS[user_version..] {
        tx.execute_batch(script)?;
    }
    // Update pragma
    tx.execute(
        &format!("PRAGMA user_version = {}", VERSION_SCRIPTS.len()),
        [],
    )?;
    tx.commit()?;
    Ok(())
}
