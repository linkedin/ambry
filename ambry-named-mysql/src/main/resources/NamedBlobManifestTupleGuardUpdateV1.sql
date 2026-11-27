CREATE TRIGGER named_blob_manifest_tuple_guard_upd_v1
AFTER UPDATE ON named_blobs_v2
FOR EACH ROW
BEGIN
    DECLARE existing_root VARBINARY(50);
    DECLARE existing_name VARBINARY(1400);
    -- Only an identity-moving update (root or any key column) needs to consult the manifest table at all: the
    -- retirement fence's own update only ever touches retirement_fenced/deleted_ts, so it never takes this path
    -- or this table's lock, in either trigger.
    IF NEW.blob_id <> OLD.blob_id OR NEW.account_id <> OLD.account_id OR NEW.container_id <> OLD.container_id
        OR BINARY NEW.blob_name <> BINARY OLD.blob_name OR NEW.version <> OLD.version THEN
        SELECT root_id,blob_name INTO existing_root,existing_name FROM named_blob_manifests_v1
            WHERE account_id = NEW.account_id AND container_id = NEW.container_id
              AND blob_name = NEW.blob_name AND version = NEW.version
            FOR UPDATE;
        IF existing_root IS NOT NULL OR existing_name <> BINARY NEW.blob_name THEN
            SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Named version is reserved';
        END IF;
    END IF;
END
