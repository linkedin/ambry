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

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.utils.TestUtils;
import java.io.IOException;
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
    PartitionId referencePartitionId = referenceClusterMap.getWritablePartitionIds().get(0);
    boolean referenceIsEncrypted = TestUtils.RANDOM.nextBoolean();
    BlobId blobId =
        new BlobId(BlobId.BLOB_ID_V3, referenceType, referenceDatacenterId, referenceAccountId, referenceContainerId,
            referencePartitionId, referenceIsEncrypted);
    BlobId regeneratedBlobId = FrontendUtils.getBlobIdFromString(blobId.getID(), referenceClusterMap);
    assertEquals("BlobId mismatch", blobId, regeneratedBlobId);

    // bad path
    try {
      FrontendUtils.getBlobIdFromString(blobId.getID().substring(1), referenceClusterMap);
      fail("Should have thrown exception for bad blobId ");
    } catch (RestServiceException e) {
      assertEquals("RestServiceErrorCode mismatch", RestServiceErrorCode.BadRequest, e.getErrorCode());
    }
  }
}
