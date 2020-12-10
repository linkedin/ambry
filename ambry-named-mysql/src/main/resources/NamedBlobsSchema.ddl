CREATE TABLE IF NOT EXISTS named_blobs (
    account_id int NOT NULL,
    container_id int NOT NULL,
    blob_name varchar(191) NOT NULL,
    blob_id varbinary(50) NOT NULL,
    deleted_ts datetime(6) DEFAULT NULL,
    expires_ts datetime(6) DEFAULT NULL,
    PRIMARY KEY (account_id, container_id, blob_name)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_bin
COMMENT='Holds mappings between blob names and blob IDs';

/* Soft Delete Index */
CREATE INDEX named_blobs_dt ON named_blobs(deleted_ts);

/* Expired Index */
CREATE INDEX named_blobs_et ON named_blobs(expires_ts);

/* Reverse Lookup Index */
CREATE INDEX named_blobs_id ON named_blobs(blob_id);
