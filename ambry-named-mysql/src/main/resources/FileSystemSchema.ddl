CREATE TABLE IF NOT EXISTS directory_info (
    account_id      INT           NOT NULL,
    container_id    INT           NOT NULL,
    parent_dir_id   BINARY(16)    NOT NULL,
    resource_name   VARCHAR(128)  NOT NULL,
    resource_type   TINYINT       NOT NULL,  -- TINYINT is suitable for small numeric values
    resource_id     BINARY(16)    NOT NULL,
    created_ts      DATETIME(6)   NOT NULL,
    last_updated_ts DATETIME(6)   NOT NULL,
    deleted_ts      DATETIME(6)   DEFAULT NULL,
    gg_modi_ts      DATETIME(6)   DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),  -- Auto-updates on modification
    gg_status       CHAR(1)       DEFAULT 'o',  -- CHAR(1) is more suitable for single-character values
    PRIMARY KEY (account_id, container_id, parent_dir_id, resource_name)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_bin
COMMENT='Holds metadata for ambry file system';

CREATE INDEX idx_directory_parent ON directory_info (parent_dir_id);

CREATE UNIQUE INDEX idx_directory_resource ON directory_info (resource_id);

CREATE INDEX idx_directory_account_container ON directory_info (account_id, container_id);

CREATE TABLE IF NOT EXISTS file_info (
      account_id      int           NOT NULL,
      container_id    int           NOT NULL,
      file_id         BINARY(16)    NOT NULL,
      blob_id         varbinary(50) NOT NULL,
      deleted_ts      DATETIME(6)   DEFAULT NULL,
      gg_modi_ts      DATETIME(6)   DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),  -- Auto-updates on modification,
      gg_status       CHAR(1)    DEFAULT 'o',
      PRIMARY KEY (account_id, container_id, file_id)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_bin
COMMENT='Holds metadata for ambry files';

CREATE UNIQUE INDEX idx_file_id ON file_info (file_id);

CREATE INDEX idx_blob_id ON file_info (blob_id); -- Optional, only if blob_id is frequently queried.

