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
import com.github.ambry.utils.Time;
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

    long maxSupportedTimeInMs = Utils.maxEpochTimeInMs;
    // valid TTLs
    long[] validTTLs = new long[]{
        1 * Time.SecsPerHour,
        10 * Time.SecsPerHour,
        100 * Time.SecsPerHour,
        1 * Time.SecsPerDay,
        10 * Time.SecsPerDay,
        100 * Time.SecsPerDay,
        12 * 30 * Time.SecsPerDay, 10 * 12 * 30 * Time.SecsPerDay, TimeUnit.MILLISECONDS.toSeconds(
        maxSupportedTimeInMs - creationTimeMs - 1), TimeUnit.MILLISECONDS.toSeconds(
        maxSupportedTimeInMs - creationTimeMs)};

    for (long ttl : validTTLs) {
      blobProperties = new BlobProperties(blobSize, serviceId, ownerId, contentType, true, ttl, creationTimeMs);
      verifyBlobProperties(blobProperties, blobSize, serviceId, ownerId, contentType, true, ttl);
    }

    // invalid TTLs
    long[] invalidTTLs = new long[]{TimeUnit.MILLISECONDS.toSeconds(
        maxSupportedTimeInMs - creationTimeMs + 1 * Time.MsPerSec), TimeUnit.MILLISECONDS.toSeconds(
        maxSupportedTimeInMs - creationTimeMs + 10 * Time.MsPerSec), TimeUnit.MILLISECONDS.toSeconds(
        maxSupportedTimeInMs - creationTimeMs + 100 * Time.MsPerSec), TimeUnit.MILLISECONDS.toSeconds(
        maxSupportedTimeInMs - creationTimeMs + 1000 * Time.MsPerSec)};
    for (long ttl : invalidTTLs) {
      blobProperties = new BlobProperties(blobSize, serviceId, ownerId, contentType, true, ttl, creationTimeMs);
      verifyBlobProperties(blobProperties, blobSize, serviceId, ownerId, contentType, true, -1);
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
