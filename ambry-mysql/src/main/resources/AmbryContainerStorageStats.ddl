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

/**
 * This table is created to store container storage usage from each individual ambry server host.
 */
CREATE TABLE IF NOT EXISTS AccountReports
(
    clusterName          VARCHAR(25) NOT NULL,
    hostname             VARCHAR(30) NOT NULL,
    partitionId          INT         NOT NULL,
    accountId            INT         NOT NULL,
    containerId          INT         NOT NULL,
    storageUsage         BIGINT      NOT NULL, /* logical storage usage */
    physicalStorageUsage BIGINT      NOT NULL DEFAULT 0, /* physical storage usage */
    numberOfBlobs        BIGINT      NOT NULL DEFAULT 0, /* number of blobs, include blobs at all states, even for compaction */
    updatedAt            TIMESTAMP   NOT NULL,

    PRIMARY KEY (clusterName, hostname, partitionId, accountId, containerId),
    INDEX updatedAtIndex (updatedAt)
)
    CHARACTER SET utf8
    COLLATE utf8_bin;

/**
  We have to add physicalStorageUsage and numberOfBlobs to the existing AccountReports table.
  We would like to set physicalStorageUsage's value to be the same as storageUsage and the numberOfBlobs to be 0

  ALTER TABLE AccountReports ADD numberOfBlobs BIGINT NOT NULL DEFAULT 0;
  ALTER TABLE AccountReports ADD physicalStorageUsage BIGINT NOT NULL DEFAULT 0;
  Update AccountReports SET physicalStorageUsage = storageUsage;
*/

/**
 * This table is created to store aggregated account storage usage. An aggregation task will be activated to read all
 * the container storage usages from table AcountReports and generate an aggregated stats and write it back to this table.
 */
CREATE TABLE IF NOT EXISTS AggregatedAccountReports
(
    clusterName          VARCHAR(25) NOT NULL,
    accountId            INT         NOT NULL,
    containerId          INT         NOT NULL,
    storageUsage         BIGINT      NOT NULL, /* logical storage usage */
    physicalStorageUsage BIGINT      NOT NULL DEFAULT 0, /* physical storage usage */
    numberOfBlobs        BIGINT      NOT NULL DEFAULT 0,
    updatedAt            TIMESTAMP   NOT NULL,

    PRIMARY KEY (clusterName, accountId, containerId)
)
    CHARACTER SET utf8
    COLLATE utf8_bin;

/**
  We have to add physicalStorageUsage and numberOfBlobs to the existing AggregatedAccountReports table.
  We would like to set physicalStorageUsage's value to be the same as storageUsage and the numberOfBlobs to be 0

  ALTER TABLE AggregatedAccountReports ADD numberOfBlobs BIGINT NOT NULL DEFAULT 0;
  ALTER TABLE AggregatedAccountReports ADD physicalStorageUsage BIGINT NOT NULL DEFAULT 0;
  Update AggregatedAccountReports SET physicalStorageUsage = storageUsage;
 */

/**
 * This table is created to keep a copy of aggregated container storage usage from table AggregatedAccountReports at the
 * beginning of each month. The schema of this table is the same as table AggregatedAccountReports. The data is directly
 * copied it every month.
 */
CREATE TABLE IF NOT EXISTS MonthlyAggregatedAccountReports LIKE AggregatedAccountReports;
/**
  ALTER TABLE MonthlyAggregatedAccountReports ADD numberOfBlobs BIGINT NOT NULL DEFAULT 0;
  ALTER TABLE MonthlyAggregatedAccountReports ADD physicalStorageUsage BIGINT NOT NULL DEFAULT 0;
  Update MonthlyAggregatedAccountReports SET physicalStorageUsage = storageUsage;
 */

/**
 * This table is created to record when the data in table MonthlyAggregatedAccountReports is copied.
 */
CREATE TABLE IF NOT EXISTS AggregatedAccountReportsMonth
(
    clusterName VARCHAR(25) NOT NULL PRIMARY KEY,
    month       VARCHAR(25) NOT NULL
)
    CHARACTER SET utf8
    COLLATE utf8_bin;

/**
 * This table is created to keep a list of partition class names. In "prod" cluster, we have "default".
 * In "test" cluster, we have "default" and "new-replication". For these three classes, we would have rows below:
 * ------------------------------------
 * clusterName  |name             |id |
 * ------------------------------------
 * prod         |default          |1  |
 * test         |default          |2  |
 * test         |new-replication  |3  |
 * ------------------------------------
 */
CREATE TABLE IF NOT EXISTS PartitionClassNames
(
    id          SMALLINT    NOT NULL PRIMARY KEY AUTO_INCREMENT,
    clusterName VARCHAR(25) NOT NULL,
    name        VARCHAR(45) NOT NULL,

    UNIQUE KEY (clusterName, name)
)
    CHARACTER SET utf8
    COLLATE utf8_bin;

/**
 * This table is created to store all the partition ids and its corresponding partition class id.
 */
CREATE TABLE IF NOT EXISTS Partitions
(
    clusterName      VARCHAR(25) NOT NULL,
    id               INT         NOT NULL,
    partitionClassId SMALLINT    NOT NULL,

    PRIMARY KEY (clusterName, id)
)
    CHARACTER SET utf8
    COLLATE utf8_bin;

/**
 * This table is created to store the aggregated partition class report.
 */
CREATE TABLE IF NOT EXISTS AggregatedPartitionClassReports
(
    partitionClassId     SMALLINT  NOT NULL,
    accountId            INT       NOT NULL,
    containerId          INT       NOT NULL,
    storageUsage         BIGINT    NOT NULL,
    physicalStorageUsage BIGINT    NOT NULL DEFAULT 0, /* physical storage usage */
    numberOfBlobs        BIGINT    NOT NULL DEFAULT 0,
    updatedAt            TIMESTAMP NOT NULL,

    PRIMARY KEY (partitionClassId, accountId, containerId)
)
    CHARACTER SET utf8
    COLLATE utf8_bin;

/**
  We have to add physicalStorageUsage and numberOfBlobs to the existing AggregatedPartitionClassReports table.
  We would like to set physicalStorageUsage's value to be the same as storageUsage and the numberOfBlobs to be 0

  ALTER TABLE AggregatedPartitionClassReports ADD numberOfBlobs BIGINT NOT NULL DEFAULT 0;
  ALTER TABLE AggregatedPartitionClassReports ADD physicalStorageUsage BIGINT NOT NULL DEFAULT 0;
  Update AggregatedPartitionClassReports SET physicalStorageUsage = storageUsage;
 */