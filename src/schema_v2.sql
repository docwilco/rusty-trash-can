ALTER TABLE channel_settings
    ADD COLUMN parent_id INTEGER REFERENCES channel_settings (channel_id);
