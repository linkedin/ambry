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

/* CREATE DATABASE AmbryRepairRequests; */

/**
 * This table is created to store the Ambry requests which were partially failed and need to get repaired.
 */
CREATE TABLE IF NOT EXISTS ambry_repair_requests (
    blobId varbinary(50) NOT NULL,
    partitionId BIGINT NOT NULL,
    sourceHostName varchar(50) NOT NULL,
    sourceHostPort int NOT NULL,
    operationType smallint NOT NULL,
    operationTime datetime(6) NOT NULL,
    lifeVersion smallint NOT NULL,
    expirationTime datetime(6) DEFAULT NULL,
    PRIMARY KEY (blobId, operationType),
    INDEX operationTimeIndex (partitionId, operationTime)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_bin
COMMENT='Holds the Ambry requests need to be repaired.';
