CREATE TABLE named_blob_manifests_v1 (
    account_id INT NOT NULL,
    container_id INT NOT NULL,
    blob_name VARCHAR(350) NOT NULL,
    version BIGINT NOT NULL,
    root_id VARBINARY(50) NOT NULL,
    child_count INT NOT NULL,
    payload LONGBLOB DEFAULT NULL,
    payload_hash BINARY(32) DEFAULT NULL,
    publication_key BINARY(32) DEFAULT NULL,
    PRIMARY KEY (account_id, container_id, blob_name, version)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

ALTER TABLE named_blobs_v2
    ADD COLUMN retirement_fenced BOOLEAN NOT NULL DEFAULT FALSE,
    ALGORITHM=INSTANT;

ALTER TABLE named_blobs_v2
    ADD INDEX named_blob_unfenced_versions (account_id, container_id, blob_name, retirement_fenced, version),
    ALGORITHM=INPLACE, LOCK=NONE;
