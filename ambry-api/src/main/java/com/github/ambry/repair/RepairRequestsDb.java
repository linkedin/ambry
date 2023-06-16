/**
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
 *
 */
package com.github.ambry.repair;

import java.io.Closeable;
import java.util.List;
import javax.sql.DataSource;

import static com.github.ambry.repair.RepairRequestRecord.OperationType;


/**
 * RepairRequestsDb
 * The database interface to manipulate the AmbryRepairRequests DB.
 * The AmbryRepairRequests DB has the records of {@link RepairRequestRecord}.
 * These are partially failed requests which need to get fixed.
 */
public interface RepairRequestsDb extends Closeable {

  /**
   * Remove one {@link RepairRequestRecord} from the database
   * @param blobId the blob id
   * @param type the operation time, either TtlUpdate or Delete
   */
  void removeRepairRequests(String blobId, OperationType type) throws Exception;

  /**
   * Insert one {@link RepairRequestRecord}
   * @param record the record to insert
   */
  void putRepairRequests(RepairRequestRecord record) throws Exception;

  /**
   * Select the records from one partition ordered by the operation time.
   * @param partitionId partition id
   * @return the oldest {@link RepairRequestRecord}s.
   */
  List<RepairRequestRecord> getRepairRequests(long partitionId) throws Exception;

  /**
   * Exposed for integration test usage.
   * @return a {@link DataSource}.
   */
  DataSource getDataSource();
}
