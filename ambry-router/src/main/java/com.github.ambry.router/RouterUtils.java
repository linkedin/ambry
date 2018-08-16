/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.router;

import com.github.ambry.account.Account;
import com.github.ambry.account.AccountService;
import com.github.ambry.account.Container;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This is a utility class used by Router.
 */
class RouterUtils {

  private static Logger logger = LoggerFactory.getLogger(RouterUtils.class);

  /**
   * Get {@link BlobId} from a blob string.
   * @param blobIdString The string of blobId.
   * @param clusterMap The {@link ClusterMap} based on which to generate the {@link BlobId}.
   * @return BlobId
   * @throws RouterException If parsing a string blobId fails.
   */
  static BlobId getBlobIdFromString(String blobIdString, ClusterMap clusterMap) throws RouterException {
    BlobId blobId;
    try {
      blobId = new BlobId(blobIdString, clusterMap);
      logger.trace("BlobId {} created with partitionId {}", blobId, blobId.getPartition());
    } catch (Exception e) {
      logger.trace("Caller passed in invalid BlobId {}", blobIdString);
      throw new RouterException("BlobId is invalid " + blobIdString, RouterErrorCode.InvalidBlobId);
    }
    return blobId;
  }

  /**
   * Checks if the given {@link ReplicaId} is in a different data center relative to a router with the
   * given {@link RouterConfig}
   * @param routerConfig the {@link RouterConfig} associated with a router
   * @param replicaId the {@link ReplicaId} whose status (local/remote) is to be determined.
   * @return true if the replica is remote, false otherwise.
   */
  static boolean isRemoteReplica(RouterConfig routerConfig, ReplicaId replicaId) {
    return !routerConfig.routerDatacenterName.equals(replicaId.getDataNodeId().getDatacenterName());
  }

  /**
   * Determine if an error is indicative of the health of the system, and not a user error.
   * If it is a system health error, then the error is logged.
   * @param exception The {@link Exception} to check.
   * @return true if this is an internal error and not a user error; false otherwise.
   */
  static boolean isSystemHealthError(Exception exception) {
    boolean isSystemHealthError = true;
    if (exception instanceof RouterException) {
      RouterErrorCode routerErrorCode = ((RouterException) exception).getErrorCode();
      switch (routerErrorCode) {
        // The following are user errors. Only increment the respective error metric.
        case InvalidBlobId:
        case InvalidPutArgument:
        case BlobTooLarge:
        case BadInputChannel:
        case BlobDeleted:
        case BlobExpired:
        case BlobAuthorizationFailure:
        case BlobDoesNotExist:
        case RangeNotSatisfiable:
        case ChannelClosed:
        case BlobUpdateNotAllowed:
          isSystemHealthError = false;
          break;
      }
    } else if (Utils.isPossibleClientTermination(exception)) {
      isSystemHealthError = false;
    }
    if (isSystemHealthError) {
      logger.error("Router operation met with a system health error: ", exception);
    }
    return isSystemHealthError;
  }

  /**
   * Return the number of data chunks for the given blob and chunk sizes.
   * @param blobSize the size of the overall blob.
   * @param chunkSize the size of each data chunk (except, possibly the last one).
   * @return the number of data chunks for the given blob and chunk sizes.
   */
  static int getNumChunksForBlobAndChunkSize(long blobSize, int chunkSize) {
    return (int) (blobSize == 0 ? 1 : (blobSize - 1) / chunkSize + 1);
  }

  /**
   * Return {@link Account} and {@link Container} in a {@link Pair}.
   * @param accountService the accountService to translate accountId to name.
   * @param accountId the accountId to translate.
   * @return {@link Account} and {@link Container} in a {@link Pair}.
   */
  static Pair<Account, Container> getAccountContainer(AccountService accountService, short accountId,
      short containerId) {
    Account account = accountService.getAccountById(accountId);
    Container container = account == null ? null : account.getContainerById(containerId);
    return new Pair<>(account, container);
  }
}
