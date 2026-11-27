CREATE TRIGGER named_blob_manifest_tuple_guard_ins_v1
AFTER INSERT ON named_blobs_v2
FOR EACH ROW
BEGIN
    DECLARE existing_root VARBINARY(50);
    DECLARE existing_name VARBINARY(1400);
    -- A manifest row is only ever inserted fully formed, in the same transaction that retires its named version:
    -- its mere existence means the tuple is published, regardless of root. Running after the base row lands
    -- means an ordinary named PUT locks named_blobs_v2 first and this table second, the same order retire uses,
    -- so the two never wait on each other in opposite directions.
    SELECT root_id,blob_name INTO existing_root,existing_name FROM named_blob_manifests_v1
        WHERE account_id = NEW.account_id AND container_id = NEW.container_id
          AND blob_name = NEW.blob_name AND version = NEW.version
        FOR UPDATE;
    -- utf8mb4_bin on VARCHAR still compares under PAD SPACE rules, so an index match can hide a trailing-space
    -- alias; compare the stored name byte-for-byte before trusting the lookup.
    IF existing_root IS NOT NULL OR existing_name <> BINARY NEW.blob_name THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Named version is reserved';
    END IF;
END
