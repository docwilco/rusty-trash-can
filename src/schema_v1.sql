CREATE TABLE channel_settings (
    channel_id INTEGER PRIMARY KEY,
    max_age INTEGER,
    max_messages INTEGER,
    last_seen_message INTEGER,
    bot_start_message INTEGER
);

CREATE TABLE message_ids (
    channel_id INTEGER,
    message_id INTEGER,
    PRIMARY KEY (channel_id, message_id)
);

CREATE TABLE IF NOT EXISTS message_log (
    channel_id INTEGER,
    message_id INTEGER,
    PRIMARY KEY (channel_id, message_id)
);
