/**
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
package com.github.ambry.cloud;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.BlobId;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.Before;
import org.junit.Test;

import static com.github.ambry.commons.BlobId.*;
import static org.junit.Assert.*;


/** Test for {@link CloudBlobMetadata} class. */
public class CloudBlobMetadataTest {

  private static final ObjectMapper mapperObj = new ObjectMapper();
  private final byte dataCenterId = 66;
  private final short accountId = 101;
  private final short containerId = 5;
  private final long partition = 666;
  private BlobId blobId;
  private final long now = System.currentTimeMillis();
  private final long futureTime = now + TimeUnit.DAYS.toMillis(7);

  @Before
  public void setup() throws Exception {
    PartitionId partitionId = new MockPartitionId(partition, MockClusterMap.DEFAULT_PARTITION_CLASS);
    blobId = new BlobId(BLOB_ID_V6, BlobIdType.NATIVE, dataCenterId, accountId, containerId, partitionId, false,
        BlobDataType.DATACHUNK);
  }

  /** Barebones constructor */
  @Test
  public void testBarebones() throws Exception {
    // Barebones constructor
    CloudBlobMetadata blobMetadata = new CloudBlobMetadata(blobId);
    verifySerde(blobMetadata, CloudBlobMetadata.REQUIRED_FIELDS,
        ArrayUtils.addAll(CloudBlobMetadata.OPTIONAL_FIELDS, CloudBlobMetadata.ENCRYPTION_FIELDS));
  }

  /** Permanent blob */
  @Test
  public void testPermanent() throws Exception {
    CloudBlobMetadata blobMetadata =
        new CloudBlobMetadata(blobId, now, -1, 1024, CloudBlobMetadata.EncryptionOrigin.NONE);
    verifySerde(blobMetadata, ArrayUtils.add(CloudBlobMetadata.REQUIRED_FIELDS, CloudBlobMetadata.FIELD_CREATION_TIME),
        ArrayUtils.addAll(CloudBlobMetadata.ENCRYPTION_FIELDS, CloudBlobMetadata.FIELD_EXPIRATION_TIME,
            CloudBlobMetadata.FIELD_DELETION_TIME));
  }

  /** TTL blob */
  @Test
  public void testExpiration() throws Exception {
    CloudBlobMetadata blobMetadata =
        new CloudBlobMetadata(blobId, now, futureTime, 1024, CloudBlobMetadata.EncryptionOrigin.NONE);
    blobMetadata.setExpirationTime(futureTime);
    verifySerde(blobMetadata,
        ArrayUtils.addAll(CloudBlobMetadata.REQUIRED_FIELDS, CloudBlobMetadata.FIELD_CREATION_TIME,
            CloudBlobMetadata.FIELD_EXPIRATION_TIME),
        ArrayUtils.addAll(CloudBlobMetadata.ENCRYPTION_FIELDS, CloudBlobMetadata.FIELD_DELETION_TIME));
  }

  /** Deleted blob */
  @Test
  public void testDeleted() throws Exception {
    CloudBlobMetadata blobMetadata =
        new CloudBlobMetadata(blobId, now, -1, 1024, CloudBlobMetadata.EncryptionOrigin.NONE);
    blobMetadata.setDeletionTime(futureTime);
    verifySerde(blobMetadata,
        ArrayUtils.addAll(CloudBlobMetadata.REQUIRED_FIELDS, CloudBlobMetadata.FIELD_CREATION_TIME,
            CloudBlobMetadata.FIELD_DELETION_TIME),
        ArrayUtils.addAll(CloudBlobMetadata.ENCRYPTION_FIELDS, CloudBlobMetadata.FIELD_EXPIRATION_TIME));
  }

  /** Encrypted blob */
  @Test
  public void testEncrypted() throws Exception {
    CloudBlobMetadata blobMetadata =
        new CloudBlobMetadata(blobId, now, -1, 1024, CloudBlobMetadata.EncryptionOrigin.VCR, "context", "factory",
            1056);
    verifySerde(blobMetadata, ArrayUtils.addAll(CloudBlobMetadata.REQUIRED_FIELDS,
        ArrayUtils.addAll(CloudBlobMetadata.ENCRYPTION_FIELDS, CloudBlobMetadata.FIELD_CREATION_TIME)),
        new String[]{CloudBlobMetadata.FIELD_DELETION_TIME, CloudBlobMetadata.FIELD_EXPIRATION_TIME});
  }

  /**
   * Verify that the correct fields are serialized, and that deserialization produces the same record.
   * @param blobMetadata the {@link CloudBlobMetadata} to verify.
   * @param expectedFields the fields expected to be serialized.
   * @param unexpectedFields the fields expected not to be serialized.
   * @throws JsonProcessingException
   */
  private void verifySerde(CloudBlobMetadata blobMetadata, String[] expectedFields, String[] unexpectedFields)
      throws JsonProcessingException {
    HashMap<String, String> propertyMap = blobMetadata.toMap();
    for (String fieldName : expectedFields) {
      assertTrue("Expected field " + fieldName, propertyMap.containsKey(fieldName));
    }
    for (String fieldName : unexpectedFields) {
      assertFalse("Unexpected field " + fieldName, propertyMap.containsKey(fieldName));
    }

    String serializedString = mapperObj.writeValueAsString(blobMetadata);
    CloudBlobMetadata deserBlobMetadata = mapperObj.readValue(serializedString, CloudBlobMetadata.class);
    assertEquals("Expected equality", blobMetadata, deserBlobMetadata);
  }
}
