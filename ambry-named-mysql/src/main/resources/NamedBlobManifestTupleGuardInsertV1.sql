CREATE TRIGGER named_blob_manifest_tuple_guard_ins_v1
BEFORE INSERT ON named_blobs_v2
FOR EACH ROW
BEGIN
    DECLARE existing_root VARBINARY(50);
    SELECT root_id INTO existing_root FROM named_blob_manifests_v1
        WHERE account_id = NEW.account_id AND container_id = NEW.container_id
          AND blob_name = NEW.blob_name AND version = NEW.version
        FOR UPDATE;
    IF existing_root IS NOT NULL AND existing_root <> NEW.blob_id THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Named tuple already has a manifest for a different root';
    END IF;
END
