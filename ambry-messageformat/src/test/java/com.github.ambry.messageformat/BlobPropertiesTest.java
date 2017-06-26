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
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.github.ambry.account.Account.*;
import static com.github.ambry.account.Container.*;
import static org.junit.Assert.*;


/**
 * Basic tests for BlobProperties
 */
@RunWith(Parameterized.class)
public class BlobPropertiesTest {

  private final short version;

  /**
   * Running for {@link BlobPropertiesSerDe#Version1} and {@link BlobPropertiesSerDe#Version2}
   * @return an array with both the versions ({@link BlobPropertiesSerDe#Version1} and {@link BlobPropertiesSerDe#Version2}).
   */
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(new Object[][]{{BlobPropertiesSerDe.Version1}, {BlobPropertiesSerDe.Version2}});
  }

  public BlobPropertiesTest(short version) {
    this.version = version;
  }

  @Test
  public void basicTest() {
    final int blobSize = 100;
    final String serviceId = "ServiceId";
    final String ownerId = "OwnerId";
    final String contentType = "ContentType";
    final int timeToLiveInSeconds = 144;

    short accountId = LEGACY_ACCOUNT_ID;
    short containerId = LEGACY_CONTAINER_ID;
    short creatorAccountId = LEGACY_ACCOUNT_ID;
    if (version == BlobPropertiesSerDe.Version2) {
      accountId = Utils.getRandomShort(TestUtils.RANDOM);
      containerId = Utils.getRandomShort(TestUtils.RANDOM);
      creatorAccountId = Utils.getRandomShort(TestUtils.RANDOM);
    }

    BlobProperties blobProperties =
        getBlobProperties(blobSize, serviceId, accountId, containerId, creatorAccountId, version);
    System.out.println(blobProperties.toString()); // Provide example of BlobProperties.toString()
    verifyBlobProperties(blobProperties, blobSize, serviceId, null, null, false, Utils.Infinite_Time, accountId,
        containerId, creatorAccountId);
    assertTrue(blobProperties.getCreationTimeInMs() > 0);
    assertTrue(blobProperties.getCreationTimeInMs() <= System.currentTimeMillis());

    blobProperties =
        getBlobProperties(blobSize, serviceId, ownerId, contentType, true, timeToLiveInSeconds, accountId, containerId,
            creatorAccountId, version);
    System.out.println(blobProperties.toString()); // Provide example of BlobProperties.toString()
    verifyBlobProperties(blobProperties, blobSize, serviceId, ownerId, contentType, true, timeToLiveInSeconds,
        accountId, containerId, creatorAccountId);
    assertTrue(blobProperties.getCreationTimeInMs() > 0);
    assertTrue(blobProperties.getCreationTimeInMs() <= System.currentTimeMillis());

    long creationTimeMs = SystemTime.getInstance().milliseconds();
    blobProperties =
        getBlobProperties(blobSize, serviceId, ownerId, contentType, true, timeToLiveInSeconds, creationTimeMs,
            accountId, containerId, creatorAccountId, version);
    System.out.println(blobProperties.toString()); // Provide example of BlobProperties.toString()
    verifyBlobProperties(blobProperties, blobSize, serviceId, ownerId, contentType, true, timeToLiveInSeconds,
        accountId, containerId, creatorAccountId);
    assertEquals(blobProperties.getCreationTimeInMs(), creationTimeMs);

    long creationTimeInSecs = TimeUnit.MILLISECONDS.toSeconds(creationTimeMs);
    // valid TTLs
    long[] validTTLs = new long[]{TimeUnit.HOURS.toSeconds(1), TimeUnit.HOURS.toSeconds(10), TimeUnit.HOURS.toSeconds(
        100), TimeUnit.DAYS.toSeconds(1), TimeUnit.DAYS.toSeconds(10), TimeUnit.DAYS.toSeconds(
        100), TimeUnit.DAYS.toSeconds(30 * 12), TimeUnit.DAYS.toSeconds(30 * 12 * 10),
        Integer.MAX_VALUE - creationTimeInSecs - 1,
        Integer.MAX_VALUE - creationTimeInSecs,
        Integer.MAX_VALUE - creationTimeInSecs + 1,
        Integer.MAX_VALUE - creationTimeInSecs + 100, Integer.MAX_VALUE - creationTimeInSecs + 10000};

    for (long ttl : validTTLs) {
      blobProperties =
          getBlobProperties(blobSize, serviceId, ownerId, contentType, true, ttl, creationTimeMs, accountId,
              containerId, creatorAccountId, version);
      verifyBlobProperties(blobProperties, blobSize, serviceId, ownerId, contentType, true, ttl, accountId, containerId,
          creatorAccountId);
    }
  }

  /**
   * Creates {@link BlobProperties} based on the args passed for the given version
   * @param blobSize the size of the blob
   * @param serviceId the serviceId associated with the {@link BlobProperties}
   * @param ownerId the ownerId associated with the {@link BlobProperties}
   * @param contentType the contentType associated with the {@link BlobProperties}
   * @param isPrivate refers to whether the blob is private or not
   * @param timeToLiveInSeconds the time to live associated with the {@link BlobProperties} in secs
   * @param creationTimeMs creation time of the blob in ms
   * @param accountId accountId of the user who uploaded the blob
   * @param containerId containerId of the blob
   * @param creatorAccountId Issuer AccountId of the put request
   * @param version the version in which {@link BlobProperties} needs to be created
   * @return the {@link BlobProperties} thus created
   */
  private BlobProperties getBlobProperties(long blobSize, String serviceId, String ownerId, String contentType,
      boolean isPrivate, long timeToLiveInSeconds, long creationTimeMs, short accountId, short containerId,
      short creatorAccountId, short version) {
    if (version == BlobPropertiesSerDe.Version1) {
      return new BlobProperties(blobSize, serviceId, ownerId, contentType, isPrivate, timeToLiveInSeconds,
          creationTimeMs);
    } else {
      return new BlobProperties(blobSize, serviceId, ownerId, contentType, isPrivate, timeToLiveInSeconds,
          creationTimeMs, accountId, containerId, creatorAccountId);
    }
  }

  /**
   * Creates {@link BlobProperties} based on the args passed for the given version
   * @param blobSize the size of the blob
   * @param serviceId the serviceId associated with the {@link BlobProperties}
   * @param ownerId the ownerId associated with the {@link BlobProperties}
   * @param contentType the contentType associated with the {@link BlobProperties}
   * @param isPrivate refers to whether the blob is private or not
   * @param timeToLiveInSeconds the time to live associated with the {@link BlobProperties} in secs
   * @param accountId accountId of the user who uploaded the blob
   * @param containerId containerId of the blob
   * @param creatorAccountId Issuer AccountId of the put request
   * @param version the version in which {@link BlobProperties} needs to be created
   * @return the {@link BlobProperties} thus created
   */
  private BlobProperties getBlobProperties(long blobSize, String serviceId, String ownerId, String contentType,
      boolean isPrivate, long timeToLiveInSeconds, short accountId, short containerId, short creatorAccountId,
      short version) {
    if (version == BlobPropertiesSerDe.Version1) {
      return new BlobProperties(blobSize, serviceId, ownerId, contentType, isPrivate, timeToLiveInSeconds);
    } else {
      return new BlobProperties(blobSize, serviceId, ownerId, contentType, isPrivate, timeToLiveInSeconds, accountId,
          containerId, creatorAccountId);
    }
  }

  /**
   * Creates {@link BlobProperties} based on the args passed for the given version
   * @param blobSize the size of the blob
   * @param serviceId the serviceId associated with the {@link BlobProperties}
   * @param accountId accountId of the user who uploaded the blob
   * @param containerId containerId of the blob
   * @param creatorAccountId Issuer AccountId of the put request
   * @param version the version in which {@link BlobProperties} needs to be created
   * @return the {@link BlobProperties} thus created
   */
  private BlobProperties getBlobProperties(long blobSize, String serviceId, short accountId, short containerId,
      short creatorAccountId, short version) {
    if (version == BlobPropertiesSerDe.Version1) {
      return new BlobProperties(blobSize, serviceId);
    } else {
      return new BlobProperties(blobSize, serviceId, accountId, containerId, creatorAccountId);
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
   * @param accountId accountId of the user who uploaded the blob
   * @param containerId containerId of the blob
   * @param issuerAccountId Issuer AccountId of the put request
   */
  private void verifyBlobProperties(BlobProperties blobProperties, long blobSize, String serviceId, String ownerId,
      String contentType, boolean isPrivate, long ttlInSecs, short accountId, short containerId,
      short issuerAccountId) {
    assertEquals(blobProperties.getBlobSize(), blobSize);
    assertEquals(blobProperties.getServiceId(), serviceId);
    assertEquals(blobProperties.getOwnerId(), ownerId);
    assertEquals(blobProperties.getContentType(), contentType);
    assertEquals(blobProperties.isPrivate(), isPrivate);
    assertEquals(blobProperties.getTimeToLiveInSeconds(), ttlInSecs);
    assertEquals("AccountId mismatch ", accountId, blobProperties.getAccountId());
    assertEquals("ContainerId mismatch ", containerId, blobProperties.getContainerId());
    assertEquals("IssuerAccountId mismatch ", issuerAccountId, blobProperties.getCreatorAccountId());
  }
}
