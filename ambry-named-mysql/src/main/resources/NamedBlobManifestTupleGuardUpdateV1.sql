CREATE TRIGGER named_blob_manifest_tuple_guard_upd_v1
BEFORE UPDATE ON named_blobs_v2
FOR EACH ROW
BEGIN
    DECLARE existing_root VARBINARY(50);
    IF NEW.blob_id <> OLD.blob_id OR NEW.account_id <> OLD.account_id OR NEW.container_id <> OLD.container_id
        OR BINARY NEW.blob_name <> BINARY OLD.blob_name OR NEW.version <> OLD.version THEN
        SELECT root_id INTO existing_root FROM named_blob_manifests_v1
            WHERE account_id = NEW.account_id AND container_id = NEW.container_id
              AND blob_name = NEW.blob_name AND version = NEW.version
            FOR UPDATE;
        IF existing_root IS NOT NULL AND existing_root <> NEW.blob_id THEN
            SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Named tuple already has a manifest for a different root';
        END IF;
    END IF;
END
