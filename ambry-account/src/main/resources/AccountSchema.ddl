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

CREATE TABLE IF NOT EXISTS Accounts
(
    accountInfo JSON NOT NULL,
    version INT NOT NULL,
    creationTime DATETIME(3) NOT NULL,
    lastModifiedTime DATETIME(3) NOT NULL,
    accountId INT GENERATED ALWAYS AS (accountInfo->>"$.accountId") STORED NOT NULL,
    accountName VARCHAR(255) GENERATED ALWAYS AS (accountInfo->>"$.accountName") NOT NULL,
    status VARCHAR(50) GENERATED ALWAYS AS (accountInfo->>"$.status") STORED NOT NULL,
    PRIMARY KEY (accountId),
    UNIQUE INDEX uniqueName (accountName),
    INDEX lmtIndex (lastModifiedTime),
    INDEX statusIndex (status)
)
CHARACTER SET utf8 COLLATE utf8_bin;

CREATE TABLE IF NOT EXISTS Containers
(
    accountId INT NOT NULL,
    containerInfo JSON NOT NULL,
    version INT NOT NULL,
    creationTime DATETIME(3) NOT NULL,
    lastModifiedTime DATETIME(3) NOT NULL,
    containerId INT GENERATED ALWAYS AS (containerInfo->>"$.containerId") STORED NOT NULL,
    containerName VARCHAR(255) GENERATED ALWAYS AS (containerInfo->>"$.containerName") NOT NULL,
    status VARCHAR(50) GENERATED ALWAYS AS (containerInfo->>"$.status") STORED NOT NULL,
    PRIMARY KEY (accountId, containerId),
    UNIQUE INDEX uniqueName (accountId, containerName),
    INDEX lmtIndex (lastModifiedTime),
    INDEX statusIndex (status)
)
CHARACTER SET utf8 COLLATE utf8_bin;

CREATE TABLE IF NOT EXISTS Datasets (
    accountId INT NOT NULL,
    containerId INT NOT NULL,
    datasetName VARCHAR(235) NOT NULL,
    versionSchema INT NOT NULL,
    retentionCount INT DEFAULT NULL,
    retentionTimeInSeconds BIGINT NOT NULL,
    userTags JSON DEFAULT NULL,
    lastModifiedTime DATETIME(3) NOT NULL,
    delete_ts DATETIME(6) DEFAULT NULL,
    PRIMARY KEY (accountId, containerId, datasetName)
    )
CHARACTER SET utf8 COLLATE utf8_bin;

CREATE TABLE IF NOT EXISTS DatasetVersions (
    accountId INT NOT NULL,
    containerId INT NOT NULL,
    datasetName VARCHAR(235) NOT NULL,
    version BIGINT NOT NULL,
    lastModifiedTime DATETIME(3) NOT NULL,
    delete_ts DATETIME(6) DEFAULT NULL,
    PRIMARY KEY (accountId, containerId, datasetName, version)
    )
CHARACTER SET utf8 COLLATE utf8_bin;
