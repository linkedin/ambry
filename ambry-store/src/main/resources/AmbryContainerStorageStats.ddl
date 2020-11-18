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

/* CREATE DATABASE ambry_container_storage_stats; */
CREATE TABLE IF NOT EXISTS AccountReports
(
    clusterName VARCHAR(25) NOT NULL,
    hostname VARCHAR(30) NOT NULL,
    partitionId INT NOT NULL,
    accountId INT NOT NULL,
    containerId INT NOT NULL,
    storageUsage BIGINT NOT NULL,
    updatedAt TIMESTAMP NOT NULL,

    PRIMARY KEY(clusterName, hostname, partitionId, accountId, containerId),
    INDEX updatedAtIndex (updatedAt)
)
CHARACTER SET utf8 COLLATE utf8_bin;

CREATE TABLE IF NOT EXISTS AggregatedAccountReports
(
    clusterName VARCHAR(25) NOT NULL,
    accountId INT NOT NULL,
    containerId INT NOT NULL,
    storageUsage BIGINT NOT NULL,
    updatedAt TIMESTAMP NOT NULL,

    PRIMARY KEY (clusterName, accountId, containerId)
)
CHARACTER SET utf8 COLLATE utf8_bin;
