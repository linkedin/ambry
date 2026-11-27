CREATE TRIGGER named_blob_manifest_tuple_guard_ins_v1
AFTER INSERT ON named_blobs_v2
FOR EACH ROW
BEGIN
    DECLARE existing_root VARBINARY(50);
    -- A manifest row is only ever inserted fully formed, in the same transaction that retires its named version,
    -- and root_id is never NULL in any stored row: existence alone is sufficient, independent of root. Running
    -- after the base row lands means an ordinary named PUT locks named_blobs_v2 first and this table second, the
    -- same order retire uses.
    SELECT root_id INTO existing_root FROM named_blob_manifests_v1
        WHERE account_id = NEW.account_id AND container_id = NEW.container_id
          AND blob_name = NEW.blob_name AND version = NEW.version
        FOR UPDATE;
    IF existing_root IS NOT NULL THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Named version is reserved';
    END IF;
END
