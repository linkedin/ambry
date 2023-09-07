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
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

import static com.github.ambry.repair.RepairRequestRecord.*;


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
   * Select the records for one partition ordered by the operation time.
   * @param partitionId partition id
   * @return the oldest {@link RepairRequestRecord}s.
   */
  List<RepairRequestRecord> getRepairRequestsForPartition(long partitionId) throws Exception;

  /**
   * Select the records from one partition but excluding this source host
   * @param partitionId partition id
   * @param hostName the host name of the source replica to exclude
   * @param hostPort the port number of the source replica to exclude
   * @return the oldest {@link RepairRequestRecord}s.
   */
  List<RepairRequestRecord> getRepairRequestsExcludingHost(long partitionId, String hostName, int hostPort)
      throws Exception;

  Set<Long> getPartitionsNeedRepair(String sourceHostName, int sourceHostPort, List<Long> partitions)
      throws SQLException;

  /**
   * The max number of result sets it will return for each query
   * @return the max number of result sets to return
   */
  int getListMaxResults();
}
