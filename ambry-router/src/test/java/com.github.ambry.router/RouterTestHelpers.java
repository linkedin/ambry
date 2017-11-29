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

import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.commons.ServerErrorCode;
import com.github.ambry.messageformat.BlobProperties;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;


/**
 * Class with helper methods for testing the router.
 */
class RouterTestHelpers {
  /**
   * Test whether two {@link BlobProperties} have the same fields
   * @return true if the fields are equivalent in the two {@link BlobProperties}
   */
  static boolean haveEquivalentFields(BlobProperties a, BlobProperties b) {
    return a.getServiceId().equals(b.getServiceId()) && a.getOwnerId().equals(b.getOwnerId()) && a.getContentType()
        .equals(b.getContentType()) && a.isPrivate() == b.isPrivate()
        && a.getTimeToLiveInSeconds() == b.getTimeToLiveInSeconds()
        && a.getCreationTimeInMs() == b.getCreationTimeInMs() && a.getAccountId() == b.getAccountId()
        && a.getContainerId() == b.getContainerId() && a.isEncrypted() == b.isEncrypted();
  }

  /**
   * Test that an operation returns a certain result when servers return error codes corresponding
   * to {@code serverErrorCodeCounts}
   * @param serverErrorCodeCounts A map from {@link ServerErrorCode}s to a count of how many servers should be set to
   *                              that error code.
   * @param serverLayout A {@link MockServerLayout} that is used to find the {@link MockServer}s to set error codes on.
   * @param expectedError The {@link RouterErrorCode} expected, or null if no error is expected.
   * @param errorCodeChecker Performs the checks that ensure that the expected {@link RouterErrorCode} is returned .
   * @throws Exception
   */
  static void testWithErrorCodes(Map<ServerErrorCode, Integer> serverErrorCodeCounts, MockServerLayout serverLayout,
      RouterErrorCode expectedError, ErrorCodeChecker errorCodeChecker) throws Exception {
    List<ServerErrorCode> serverErrorCodes = new ArrayList<>();
    for (Map.Entry<ServerErrorCode, Integer> entry : serverErrorCodeCounts.entrySet()) {
      for (int j = 0; j < entry.getValue(); j++) {
        serverErrorCodes.add(entry.getKey());
      }
    }
    Collections.shuffle(serverErrorCodes);
    testWithErrorCodes(serverErrorCodes.toArray(new ServerErrorCode[0]), serverLayout, expectedError, errorCodeChecker);
  }

  /**
   * Test that an operation returns a certain result when each server in the layout returns a certain error code.
   * @param serverErrorCodesInOrder The error codes to set in the order of the servers in {@code serverLayout}.  If
   *                                there are more values in this array than there are servers, an exception is thrown.
   *                                If there are fewer, the rest of the servers are set to
   *                                {@link ServerErrorCode#No_Error}
   * @param serverLayout A {@link MockServerLayout} that is used to find the {@link MockServer}s to set error codes on.
   * @param expectedError The {@link RouterErrorCode} expected, or null if no error is expected.
   * @param errorCodeChecker Performs the checks that ensure that the expected {@link RouterErrorCode} is returned .
   * @throws Exception
   */
  static void testWithErrorCodes(ServerErrorCode[] serverErrorCodesInOrder, MockServerLayout serverLayout,
      RouterErrorCode expectedError, ErrorCodeChecker errorCodeChecker) throws Exception {
    setServerErrorCodes(serverErrorCodesInOrder, serverLayout);
    errorCodeChecker.testAndAssert(expectedError);
    resetServerErrorCodes(serverLayout);
  }

  /**
   * Test that an operation returns a certain result when each server in a partition returns a certain error code.
   * @param serverErrorCodesInOrder The error codes to set in the order of the servers returned by
   *                                {@link PartitionId#getReplicaIds()}. If there are more values in this array than
   *                                there are servers containing the partition, an exception is thrown. If there are
   *                                fewer, the rest of the servers are set to {@link ServerErrorCode#No_Error}
   * @param partition The partition contained by the servers to set error codes on.
   * @param serverLayout A {@link MockServerLayout} that is used to find the {@link MockServer}s to set error codes on.
   * @param expectedError The {@link RouterErrorCode} expected, or null if no error is expected.
   * @param errorCodeChecker Performs the checks that ensure that the expected {@link RouterErrorCode} is returned .
   * @throws Exception
   */
  static void testWithErrorCodes(ServerErrorCode[] serverErrorCodesInOrder, PartitionId partition,
      MockServerLayout serverLayout, RouterErrorCode expectedError, ErrorCodeChecker errorCodeChecker)
      throws Exception {
    setServerErrorCodes(serverErrorCodesInOrder, partition, serverLayout);
    errorCodeChecker.testAndAssert(expectedError);
    resetServerErrorCodes(serverLayout);
  }

  /**
   * Set the servers in the specified layout to respond with the designated error codes.
   * @param serverErrorCodesInOrder The error codes to set in the order of the servers in {@code serverLayout}.
   *                                If there are fewer error codes in this array than there are servers,
   *                                the rest of the servers are set to {@link ServerErrorCode#No_Error}
   * @param serverLayout A {@link MockServerLayout} that is used to find the {@link MockServer}s to set error codes on.
   * @throws IllegalArgumentException If there are more error codes in the input array then there are servers.
   */
  static void setServerErrorCodes(ServerErrorCode[] serverErrorCodesInOrder, MockServerLayout serverLayout) {
    Collection<MockServer> servers = serverLayout.getMockServers();
    if (serverErrorCodesInOrder.length > servers.size()) {
      throw new IllegalArgumentException("More server error codes provided than servers in cluster");
    }
    int i = 0;
    for (MockServer server : servers) {
      if (i < serverErrorCodesInOrder.length) {
        server.setServerErrorForAllRequests(serverErrorCodesInOrder[i++]);
      } else {
        server.setServerErrorForAllRequests(ServerErrorCode.No_Error);
      }
    }
  }

  /**
   * Set the servers containing the specified partition to respond with the designated error codes.
   * @param serverErrorCodesInOrder The error codes to set in the order of the servers returned by
   *                                {@link PartitionId#getReplicaIds()}. If there are fewer error codes in this array
   *                                than there are servers containing the partition, the rest of the servers are set to
   *                                {@link ServerErrorCode#No_Error}
   * @param partition The partition contained by the servers to set error codes on.
   * @param serverLayout A {@link MockServerLayout} that is used to find the {@link MockServer}s to set error codes on.
   * @throws IllegalArgumentException If there are more error codes in the input array then there are servers.
   */
  static void setServerErrorCodes(ServerErrorCode[] serverErrorCodesInOrder, PartitionId partition,
      MockServerLayout serverLayout) {
    List<? extends ReplicaId> replicas = partition.getReplicaIds();
    if (serverErrorCodesInOrder.length > replicas.size()) {
      throw new IllegalArgumentException("More server error codes provided than replicas in partition");
    }
    int i = 0;
    for (ReplicaId replica : partition.getReplicaIds()) {
      DataNodeId node = replica.getDataNodeId();
      MockServer mockServer = serverLayout.getMockServer(node.getHostname(), node.getPort());
      mockServer.setServerErrorForAllRequests(serverErrorCodesInOrder[i++]);
    }
  }

  /**
   * Reset all preset error codes on the servers in the specified layout.
   * @param serverLayout A {@link MockServerLayout} that is used to find the {@link MockServer}s to reset error codes
   *                     on.
   */
  static void resetServerErrorCodes(MockServerLayout serverLayout) {
    for (MockServer server : serverLayout.getMockServers()) {
      server.resetServerErrors();
    }
  }

  /**
   * Set the blob format version that the server should respond to get requests with.
   * @param blobFormatVersion The blob format version to use.
   * @param serverLayout A {@link MockServerLayout} containing the {@link MockServer}s to change settings on.
   */
  static void setBlobFormatVersionForAllServers(short blobFormatVersion, MockServerLayout serverLayout) {
    ArrayList<MockServer> mockServers = new ArrayList<>(serverLayout.getMockServers());
    for (MockServer mockServer : mockServers) {
      mockServer.setBlobFormatVersion(blobFormatVersion);
    }
  }

  /**
   * Set whether all servers should send the preset error code on data blob get requests
   * only
   * @param getErrorOnDataBlobOnly {@code true} if the preset error code should only be used for data blobs requests
   * @param serverLayout A {@link MockServerLayout} containing the {@link MockServer}s to change settings on.
   */
  static void setGetErrorOnDataBlobOnlyForAllServers(boolean getErrorOnDataBlobOnly, MockServerLayout serverLayout) {
    ArrayList<MockServer> mockServers = new ArrayList<>(serverLayout.getMockServers());
    for (MockServer mockServer : mockServers) {
      mockServer.setGetErrorOnDataBlobOnly(getErrorOnDataBlobOnly);
    }
  }

  /**
   * Implement this interface to provide {@link #testWithErrorCodes} with custom verification logic.
   */
  interface ErrorCodeChecker {
    /**
     * Test and make assertions related to the expected error code.
     * @param expectedError The {@link RouterErrorCode} that an operation is expected to report, or {@code null} if no
     *                      error is expected.
     * @throws Exception
     */
    void testAndAssert(RouterErrorCode expectedError) throws Exception;
  }
}

