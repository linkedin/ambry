/*
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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
CREATE TABLE IF NOT EXISTS ContainerMetadata
(
    ACCOUNT_ID INT NOT NULL,
    CONTAINER_INFO JSON NOT NULL,
    VERSION INT NOT NULL,
    CREATION_TIME DATETIME NOT NULL,
    LAST_MODIFIED_TIME DATETIME NOT NULL,
    CONTAINER_ID INT GENERATED ALWAYS AS (CONTAINER_INFO->>"$.containerId") STORED NOT NULL,
    CONTAINER_NAME VARCHAR(255) GENERATED ALWAYS AS (CONTAINER_INFO->>"$.containerName") NOT NULL,
    CONTAINER_STATUS VARCHAR(50) GENERATED ALWAYS AS (CONTAINER_INFO->>"$.status") STORED NOT NULL,
    UNIQUE KEY accountContainer (ACCOUNT_ID, CONTAINER_ID),
    UNIQUE INDEX uniqueName (ACCOUNT_ID, CONTAINER_NAME),
    INDEX lmtIndex (LAST_MODIFIED_TIME),
    INDEX statusIndex (CONTAINER_STATUS)
);
