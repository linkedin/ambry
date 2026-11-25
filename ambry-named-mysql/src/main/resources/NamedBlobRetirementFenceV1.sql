CREATE TRIGGER named_blob_retirement_fence_v1
BEFORE UPDATE ON named_blobs_v2
FOR EACH ROW
BEGIN
    IF OLD.retirement_fenced THEN
        IF NEW.deleted_ts IS NULL
           OR NEW.retirement_fenced <> OLD.retirement_fenced
           OR NEW.account_id <> OLD.account_id
           OR NEW.container_id <> OLD.container_id
           OR BINARY NEW.blob_name <> BINARY OLD.blob_name
           OR NEW.version <> OLD.version
           OR NEW.blob_id <> OLD.blob_id
           OR NEW.blob_state <> OLD.blob_state THEN
            SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Named version is durably retired';
        END IF;
        SET NEW.deleted_ts = OLD.deleted_ts;
    ELSEIF NEW.retirement_fenced THEN
        SET NEW.deleted_ts = LEAST(COALESCE(OLD.deleted_ts, UTC_TIMESTAMP(6)), UTC_TIMESTAMP(6));
    END IF;
END
