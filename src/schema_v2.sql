ALTER TABLE channel_settings
    ADD COLUMN parent_id INTEGER REFERENCES channel_settings (channel_id);

ALTER TABLE message_ids
    ADD CONSTRAINT fk_channel_id
    FOREIGN KEY (channel_id)
    REFERENCES channel_settings (channel_id);
