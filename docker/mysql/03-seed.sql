-- Minimal seed data needed by the server on an empty database.
-- ID sequencers used by gb_app.get_id_from_sequence() and gb_platform.next_platform_id().
INSERT INTO gb_app.id_sequencer (id) VALUES (0);
INSERT INTO gb_platform.id_sequencer (id) VALUES (0);
