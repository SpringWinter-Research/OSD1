//! SQLite execution worker. One connection, one thread, bounded FIFO queue.

pub fn crate_name() -> &'static str {
    "osd1-engine"
}
