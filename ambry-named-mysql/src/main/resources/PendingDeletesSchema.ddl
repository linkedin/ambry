CREATE TABLE pending_deletes (
     account_id      INT           NOT NULL,
     container_id    INT           NOT NULL,
     pending_dir_id  BINARY(16)    NOT NULL,
     insert_ts       DATETIME(6)   NOT NULL,
     deleted_ts      DATETIME(6)   NOT NULL,
     gg_modi_ts      DATETIME(6)   DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
     gg_status       CHAR(1)       DEFAULT 'o',
     PRIMARY KEY (account_id, container_id, pending_dir_id)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_bin
COMMENT='Holds pending deleted directories';

CREATE INDEX pending_deletes_insert_time
    ON pending_deletes (account_id, container_id, insert_ts);


