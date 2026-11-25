CREATE TABLE named_blob_manifests_v1 (
    manifest_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    account_id INT NOT NULL,
    container_id INT NOT NULL,
    blob_name VARCHAR(350) NOT NULL,
    version BIGINT NOT NULL,
    root_id VARBINARY(50) NOT NULL,
    child_count INT NOT NULL,
    publication_key BINARY(32) DEFAULT NULL,
    PRIMARY KEY (manifest_id),
    UNIQUE KEY manifest_named_version (account_id, container_id, blob_name, version)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE named_blob_manifest_pages_v1 (
    manifest_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    first_child INT NOT NULL,
    payload VARBINARY(8192) NOT NULL,
    page_hash BINARY(32) NOT NULL,
    PRIMARY KEY (manifest_id, first_child),
    FOREIGN KEY (manifest_id) REFERENCES named_blob_manifests_v1 (manifest_id)
) ENGINE=InnoDB;

ALTER TABLE named_blobs_v2
    ADD COLUMN retirement_fenced BOOLEAN NOT NULL DEFAULT FALSE,
    ALGORITHM=INSTANT;

ALTER TABLE named_blobs_v2
    ADD INDEX named_blob_unfenced_versions (account_id, container_id, blob_name, retirement_fenced, version),
    ALGORITHM=INPLACE, LOCK=NONE;
