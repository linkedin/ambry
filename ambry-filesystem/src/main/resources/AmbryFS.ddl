/*
 * Copyright 2023 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

/* CREATE DATABASE ambryfs; */

CREATE TABLE IF NOT EXISTS DirectoryInfo (
    account_id      int           NOT NULL,
    container_id    int           NOT NULL,
    parent_dir_id   binary(16)    NOT NULL,
    resource_name   varchar(128)  NOT NULL,
    resource_type   byte          NOT NULL,
    resource_id     binary(16)    NOT NULL,
    created_ts      datetime(6)   NOT NULL,
    last_updated_ts datetime(6)   NOT NULL,
    deleted_ts      datetime(6)   DEFAULT NULL,

    PRIMARY KEY (account_id, container_id, parent_dir_id, resource_name)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_bin
COMMENT='Holds metadata for ambry file system';

CREATE TABLE IF NOT EXISTS FileInfo (
    account_id      int           NOT NULL,
    container_id    int           NOT NULL,
    file_id         binary(16)    NOT NULL,
    blob_id         varbinary(50) NOT NULL,
    deleted_ts      datetime(6)   DEFAULT NULL,

    PRIMARY KEY (account_id, container_id, file_id)
)
    ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_bin
COMMENT='Holds metadata for ambry files';
