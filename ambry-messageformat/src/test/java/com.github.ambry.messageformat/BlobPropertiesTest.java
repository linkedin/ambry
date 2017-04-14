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
package com.github.ambry.messageformat;

import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Utils;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Basic tests for BlobProperties
 */
public class BlobPropertiesTest {
  @Test
  public void basicTest() {
    final int blobSize = 100;
    final String serviceId = "ServiceId";
    final String ownerId = "OwnerId";
    final String contentType = "ContentType";
    final int timeToLiveInSeconds = 144;

    BlobProperties blobProperties = new BlobProperties(blobSize, serviceId);
    System.out.println(blobProperties.toString()); // Provide example of BlobProperties.toString()
    verifyBlobProperties(blobProperties, blobSize, serviceId, null, null, false, Utils.Infinite_Time);
    assertTrue(blobProperties.getCreationTimeInMs() > 0);
    assertTrue(blobProperties.getCreationTimeInMs() <= System.currentTimeMillis());

    blobProperties = new BlobProperties(blobSize, serviceId, ownerId, contentType, true, timeToLiveInSeconds);
    System.out.println(blobProperties.toString()); // Provide example of BlobProperties.toString()
    verifyBlobProperties(blobProperties, blobSize, serviceId, ownerId, contentType, true, timeToLiveInSeconds);
    assertTrue(blobProperties.getCreationTimeInMs() > 0);
    assertTrue(blobProperties.getCreationTimeInMs() <= System.currentTimeMillis());

    long creationTimeMs = SystemTime.getInstance().milliseconds();
    blobProperties =
        new BlobProperties(blobSize, serviceId, ownerId, contentType, true, timeToLiveInSeconds, creationTimeMs);
    System.out.println(blobProperties.toString()); // Provide example of BlobProperties.toString()
    verifyBlobProperties(blobProperties, blobSize, serviceId, ownerId, contentType, true, timeToLiveInSeconds);
    assertEquals(blobProperties.getCreationTimeInMs(), creationTimeMs);

    long creationTimeInSecs = TimeUnit.MILLISECONDS.toSeconds(creationTimeMs);
    // valid TTLs
    long[] validTTLs = new long[]{TimeUnit.HOURS.toSeconds(1), TimeUnit.HOURS.toSeconds(10), TimeUnit.HOURS.toSeconds(
        100), TimeUnit.DAYS.toSeconds(1), TimeUnit.DAYS.toSeconds(10), TimeUnit.DAYS.toSeconds(
        100), TimeUnit.DAYS.toSeconds(30 * 12), TimeUnit.DAYS.toSeconds(30 * 12 * 10),
        Integer.MAX_VALUE - creationTimeInSecs - 1, Integer.MAX_VALUE - creationTimeInSecs};

    for (long ttl : validTTLs) {
      blobProperties = new BlobProperties(blobSize, serviceId, ownerId, contentType, true, ttl, creationTimeMs);
      verifyBlobProperties(blobProperties, blobSize, serviceId, ownerId, contentType, true, ttl);
    }

    // invalid TTLs
    long[] invalidTTLs = new long[]{
        Integer.MAX_VALUE - creationTimeInSecs + 1,
        Integer.MAX_VALUE - creationTimeInSecs + 100, Integer.MAX_VALUE - creationTimeInSecs + 10000};
    for (long ttl : invalidTTLs) {
      blobProperties = new BlobProperties(blobSize, serviceId, ownerId, contentType, true, ttl, creationTimeMs);
      verifyBlobProperties(blobProperties, blobSize, serviceId, ownerId, contentType, true, Utils.Infinite_Time);
    }
  }

  /**
   * Verify {@link BlobProperties} for its constituent values
   * @param blobProperties the {@link BlobProperties} that needs to be compared against
   * @param blobSize the size of the blob
   * @param serviceId the serviceId associated with the {@link BlobProperties}
   * @param ownerId the ownerId associated with the {@link BlobProperties}
   * @param contentType the contentType associated with the {@link BlobProperties}
   * @param isPrivate refers to whether the blob is private or not
   * @param ttlInSecs the time to live associated with the {@link BlobProperties} in secs
   */
  private void verifyBlobProperties(BlobProperties blobProperties, long blobSize, String serviceId, String ownerId,
      String contentType, boolean isPrivate, long ttlInSecs) {
    assertEquals(blobProperties.getBlobSize(), blobSize);
    assertEquals(blobProperties.getServiceId(), serviceId);
    assertEquals(blobProperties.getOwnerId(), ownerId);
    assertEquals(blobProperties.getContentType(), contentType);
    assertEquals(blobProperties.isPrivate(), isPrivate);
    assertEquals(blobProperties.getTimeToLiveInSeconds(), ttlInSecs);
  }
}
