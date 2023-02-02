/* New Table */
CREATE TABLE IF NOT EXISTS named_blobs_v2 (
    account_id int NOT NULL,
    container_id int NOT NULL,
    blob_name varchar(191) NOT NULL,
    version bigint NOT NULL,
    blob_id varbinary(50) NOT NULL,
    blob_state smallint NOT NULL,
    deleted_ts datetime(6) DEFAULT NULL,
    PRIMARY KEY (account_id, container_id, blob_name, version)
)

ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_bin
COMMENT='Holds mappings between blob names and blob IDs';

/* Soft Delete Index */
CREATE INDEX named_blobs_dt_v2 ON named_blobs_v2(deleted_ts);

/* Reverse Lookup Index */
CREATE INDEX named_blobs_id_v2 ON named_blobs_v2(blob_id);

/* Blob Name Index */
CREATE INDEX named_blobs_name_v2 ON named_blobs_v2(blob_name);
