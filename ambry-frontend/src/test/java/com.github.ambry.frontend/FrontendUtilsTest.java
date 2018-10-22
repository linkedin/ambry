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
package com.github.ambry.frontend;

import com.github.ambry.account.Account;
import com.github.ambry.account.Container;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.utils.TestUtils;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Test;

import static com.github.ambry.utils.Utils.*;
import static org.junit.Assert.*;


/**
 * Tests {@link FrontendUtils}
 */
public class FrontendUtilsTest {

  /**
   * Tests {@link FrontendUtils#getBlobIdFromString(String, ClusterMap)}
   * @throws IOException
   * @throws RestServiceException
   */
  @Test
  public void testGetBlobIdFromString() throws IOException, RestServiceException {
    // good path
    byte[] bytes = new byte[2];
    ClusterMap referenceClusterMap = new MockClusterMap();
    TestUtils.RANDOM.nextBytes(bytes);
    BlobId.BlobIdType referenceType =
        TestUtils.RANDOM.nextBoolean() ? BlobId.BlobIdType.NATIVE : BlobId.BlobIdType.CRAFTED;
    TestUtils.RANDOM.nextBytes(bytes);
    byte referenceDatacenterId = bytes[0];
    short referenceAccountId = getRandomShort(TestUtils.RANDOM);
    short referenceContainerId = getRandomShort(TestUtils.RANDOM);
    PartitionId referencePartitionId =
        referenceClusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0);
    boolean referenceIsEncrypted = TestUtils.RANDOM.nextBoolean();
    List<Short> versions = Arrays.stream(BlobId.getAllValidVersions())
        .filter(version -> version >= BlobId.BLOB_ID_V3)
        .collect(Collectors.toList());
    for (short version : versions) {
      BlobId blobId =
          new BlobId(version, referenceType, referenceDatacenterId, referenceAccountId, referenceContainerId,
              referencePartitionId, referenceIsEncrypted, BlobId.BlobDataType.DATACHUNK);
      BlobId regeneratedBlobId = FrontendUtils.getBlobIdFromString(blobId.getID(), referenceClusterMap);
      assertEquals("BlobId mismatch", blobId, regeneratedBlobId);
      assertBlobIdFieldValues(regeneratedBlobId, referenceType, referenceDatacenterId, referenceAccountId,
          referenceContainerId, referencePartitionId, version >= BlobId.BLOB_ID_V4 && referenceIsEncrypted);

      // bad path
      try {
        FrontendUtils.getBlobIdFromString(blobId.getID().substring(1), referenceClusterMap);
        fail("Should have thrown exception for bad blobId ");
      } catch (RestServiceException e) {
        assertEquals("RestServiceErrorCode mismatch", RestServiceErrorCode.BadRequest, e.getErrorCode());
      }
    }
  }

  /**
   * Asserts a {@link BlobId} against the expected values.
   * @param blobId The {@link BlobId} to assert.
   * @param type The expected {@link BlobId.BlobIdType}.
   * @param datacenterId The expected {@code datacenterId}. This will be of no effect if version is set to v1, and the
   *                     expected value will become {@link com.github.ambry.clustermap.ClusterMapUtils#UNKNOWN_DATACENTER_ID}.
   *                     For v2, {@code null} will make the assertion against
   *                     {@link com.github.ambry.clustermap.ClusterMapUtils#UNKNOWN_DATACENTER_ID}.
   * @param accountId The expected {@code accountId}. This will be of no effect if version is set to v1, and the expected
   *                  value will become {@link Account#UNKNOWN_ACCOUNT_ID}. For v2, {@code null} will make the assertion
   *                  against {@link Account#UNKNOWN_ACCOUNT_ID}.
   * @param containerId The expected {@code containerId}. This will be of no effect if version is set to v1, and the
   *                    expected value will become {@link Container#UNKNOWN_CONTAINER_ID}. For v2, {@code null} will make
   *                    the assertion against {@link Container#UNKNOWN_CONTAINER_ID}.
   * @param partitionId The expected partitionId.
   * @param isEncrypted {@code true} expected {@code isEncrypted}. This will be of no effect if version is set to v1 and v2.
   * @throws IOException Any unexpected exception.
   */
  private void assertBlobIdFieldValues(BlobId blobId, BlobId.BlobIdType type, byte datacenterId, short accountId,
      short containerId, PartitionId partitionId, boolean isEncrypted) throws IOException {
    assertEquals("Wrong partition id in blobId: " + blobId, partitionId, blobId.getPartition());
    assertEquals("Wrong type in blobId: " + blobId, type, blobId.getType());
    assertEquals("Wrong datacenter id in blobId: " + blobId, datacenterId, blobId.getDatacenterId());
    assertEquals("Wrong account id in blobId: " + blobId, accountId, blobId.getAccountId());
    assertEquals("Wrong container id in blobId: " + blobId, containerId, blobId.getContainerId());
    assertEquals("Wrong isEncrypted value in blobId: " + blobId, isEncrypted, BlobId.isEncrypted(blobId.getID()));
  }
}
