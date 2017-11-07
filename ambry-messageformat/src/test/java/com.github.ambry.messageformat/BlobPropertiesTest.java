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

import com.github.ambry.account.Account;
import com.github.ambry.account.Container;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.github.ambry.account.Account.*;
import static com.github.ambry.account.Container.*;
import static com.github.ambry.messageformat.BlobPropertiesSerDe.*;
import static com.github.ambry.messageformat.MessageFormatRecord.*;
import static org.junit.Assert.*;


/**
 * Basic tests for BlobProperties
 */
@RunWith(Parameterized.class)
public class BlobPropertiesTest {

  private final short version;

  /**
   * Running for {@link BlobPropertiesSerDe#VERSION_1} and {@link BlobPropertiesSerDe#VERSION_2}
   * @return an array with both the versions ({@link BlobPropertiesSerDe#VERSION_1} and {@link BlobPropertiesSerDe#VERSION_2}).
   */
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(
        new Object[][]{{BlobPropertiesSerDe.VERSION_1}, {BlobPropertiesSerDe.VERSION_2}, {BlobPropertiesSerDe.VERSION_3}});
  }

  public BlobPropertiesTest(short version) {
    this.version = version;
  }

  @Test
  public void basicTest() throws IOException {
    int blobSize = 100;
    String serviceId = "ServiceId";
    String ownerId = "OwnerId";
    String contentType = "ContentType";
    int timeToLiveInSeconds = 144;
    short accountId = Utils.getRandomShort(TestUtils.RANDOM);
    short containerId = Utils.getRandomShort(TestUtils.RANDOM);
    boolean isEncrypted = TestUtils.RANDOM.nextBoolean();

    short accountIdToExpect = version == BlobPropertiesSerDe.VERSION_1 ? UNKNOWN_ACCOUNT_ID : accountId;
    short containerIdToExpect = version == BlobPropertiesSerDe.VERSION_1 ? UNKNOWN_CONTAINER_ID : containerId;
    boolean encryptFlagToExpect = version == BlobPropertiesSerDe.VERSION_3 && isEncrypted;

    BlobProperties blobProperties =
        getBlobProperties(blobSize, isEncrypted, serviceId, accountId, containerId, version);
    System.out.println(blobProperties.toString()); // Provide example of BlobProperties.toString()
    ByteBuffer serializedBuffer = null;
    if (version == BlobPropertiesSerDe.VERSION_1) {
      serializedBuffer = getBlobPropsSerDeV1(blobProperties);
    } else if (version == BlobPropertiesSerDe.VERSION_2) {
      serializedBuffer = getBlobPropsSerDeV2(blobProperties);
    } else {
      serializedBuffer = ByteBuffer.allocate(BlobPropertiesSerDe.getBlobPropertiesSerDeSize(blobProperties));
      BlobPropertiesSerDe.serializeBlobProperties(serializedBuffer, blobProperties);
    }
    serializedBuffer.flip();
    blobProperties = BlobPropertiesSerDe.getBlobPropertiesFromStream(
        new DataInputStream(new ByteBufferInputStream(serializedBuffer)));
    verifyBlobProperties(blobProperties, blobSize, serviceId, "", "", false, Utils.Infinite_Time, accountIdToExpect,
        containerIdToExpect, encryptFlagToExpect);
    assertTrue(blobProperties.getCreationTimeInMs() > 0);
    assertTrue(blobProperties.getCreationTimeInMs() <= System.currentTimeMillis());

    blobProperties = getBlobProperties(blobSize, false, serviceId, accountId, containerId, version);

    if (version == BlobPropertiesSerDe.VERSION_1) {
      serializedBuffer = getBlobPropsSerDeV1(blobProperties);
    } else if (version == BlobPropertiesSerDe.VERSION_2) {
      serializedBuffer = getBlobPropsSerDeV2(blobProperties);
    } else {
      serializedBuffer = ByteBuffer.allocate(BlobPropertiesSerDe.getBlobPropertiesSerDeSize(blobProperties));
      BlobPropertiesSerDe.serializeBlobProperties(serializedBuffer, blobProperties);
    }
    serializedBuffer.flip();
    blobProperties = BlobPropertiesSerDe.getBlobPropertiesFromStream(
        new DataInputStream(new ByteBufferInputStream(serializedBuffer)));
    verifyBlobProperties(blobProperties, blobSize, serviceId, "", "", false, Utils.Infinite_Time, accountIdToExpect,
        containerIdToExpect, false);

    blobProperties =
        getBlobProperties(blobSize, isEncrypted, serviceId, ownerId, contentType, true, timeToLiveInSeconds, accountId,
            containerId, version);
    System.out.println(blobProperties.toString()); // Provide example of BlobProperties.toString()

    if (version == BlobPropertiesSerDe.VERSION_1) {
      serializedBuffer = getBlobPropsSerDeV1(blobProperties);
    } else if (version == BlobPropertiesSerDe.VERSION_2) {
      serializedBuffer = getBlobPropsSerDeV2(blobProperties);
    } else {
      serializedBuffer = ByteBuffer.allocate(BlobPropertiesSerDe.getBlobPropertiesSerDeSize(blobProperties));
      BlobPropertiesSerDe.serializeBlobProperties(serializedBuffer, blobProperties);
    }
    serializedBuffer.flip();
    blobProperties = BlobPropertiesSerDe.getBlobPropertiesFromStream(
        new DataInputStream(new ByteBufferInputStream(serializedBuffer)));
    verifyBlobProperties(blobProperties, blobSize, serviceId, ownerId, contentType, true, timeToLiveInSeconds,
        accountIdToExpect, containerIdToExpect, encryptFlagToExpect);
    assertTrue(blobProperties.getCreationTimeInMs() > 0);
    assertTrue(blobProperties.getCreationTimeInMs() <= System.currentTimeMillis());

    long creationTimeMs = SystemTime.getInstance().milliseconds();
    blobProperties =
        getBlobProperties(blobSize, isEncrypted, serviceId, ownerId, contentType, true, timeToLiveInSeconds,
            creationTimeMs, accountId, containerId, version);
    System.out.println(blobProperties.toString()); // Provide example of BlobProperties.toString()
    if (version == BlobPropertiesSerDe.VERSION_1) {
      serializedBuffer = getBlobPropsSerDeV1(blobProperties);
    } else if (version == BlobPropertiesSerDe.VERSION_2) {
      serializedBuffer = getBlobPropsSerDeV2(blobProperties);
    } else {
      serializedBuffer = ByteBuffer.allocate(BlobPropertiesSerDe.getBlobPropertiesSerDeSize(blobProperties));
      BlobPropertiesSerDe.serializeBlobProperties(serializedBuffer, blobProperties);
    }
    serializedBuffer.flip();

    blobProperties = BlobPropertiesSerDe.getBlobPropertiesFromStream(
        new DataInputStream(new ByteBufferInputStream(serializedBuffer)));
    verifyBlobProperties(blobProperties, blobSize, serviceId, ownerId, contentType, true, timeToLiveInSeconds,
        accountIdToExpect, containerIdToExpect, encryptFlagToExpect);
    assertEquals(blobProperties.getCreationTimeInMs(), creationTimeMs);

    // testValid TTLs only for latest version
    if (version == BlobPropertiesSerDe.VERSION_3) {
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
            getBlobProperties(blobSize, isEncrypted, serviceId, ownerId, contentType, true, ttl, creationTimeMs,
                accountId, containerId, version);
        serializedBuffer = ByteBuffer.allocate(BlobPropertiesSerDe.getBlobPropertiesSerDeSize(blobProperties));
        BlobPropertiesSerDe.serializeBlobProperties(serializedBuffer, blobProperties);
        serializedBuffer.flip();
        blobProperties = BlobPropertiesSerDe.getBlobPropertiesFromStream(
            new DataInputStream(new ByteBufferInputStream(serializedBuffer)));
        verifyBlobProperties(blobProperties, blobSize, serviceId, ownerId, contentType, true, ttl, accountIdToExpect,
            containerIdToExpect, encryptFlagToExpect);
      }
    }
  }

  /**
   * Creates {@link BlobProperties} based on the args passed for the given version
   * @param blobSize the size of the blob
   * @param isEncrypted whether the blob is encrypted
   * @param serviceId the serviceId associated with the {@link BlobProperties}
   * @param ownerId the ownerId associated with the {@link BlobProperties}
   * @param contentType the contentType associated with the {@link BlobProperties}
   * @param isPrivate refers to whether the blob is private or not
   * @param timeToLiveInSeconds the time to live associated with the {@link BlobProperties} in secs
   * @param creationTimeMs creation time of the blob in ms
   * @param accountId accountId of the user who uploaded the blob
   * @param containerId containerId of the blob
   * @param version the version in which {@link BlobProperties} needs to be created
   * @return the {@link BlobProperties} thus created
   */
  private BlobProperties getBlobProperties(long blobSize, boolean isEncrypted, String serviceId, String ownerId,
      String contentType, boolean isPrivate, long timeToLiveInSeconds, long creationTimeMs, short accountId,
      short containerId, short version) {
    if (version == BlobPropertiesSerDe.VERSION_1) {
      return new BlobProperties(blobSize, serviceId, ownerId, contentType, isPrivate, timeToLiveInSeconds,
          creationTimeMs, Account.UNKNOWN_ACCOUNT_ID, Container.UNKNOWN_CONTAINER_ID, isEncrypted);
    } else {
      return new BlobProperties(blobSize, serviceId, ownerId, contentType, isPrivate, timeToLiveInSeconds,
          creationTimeMs, accountId, containerId, isEncrypted);
    }
  }

  /**
   * Creates {@link BlobProperties} based on the args passed for the given version
   * @param blobSize the size of the blob
   * @param isEncrypted whether the blob is encrypted
   * @param serviceId the serviceId associated with the {@link BlobProperties}
   * @param ownerId the ownerId associated with the {@link BlobProperties}
   * @param contentType the contentType associated with the {@link BlobProperties}
   * @param isPrivate refers to whether the blob is private or not
   * @param timeToLiveInSeconds the time to live associated with the {@link BlobProperties} in secs
   * @param accountId accountId of the user who uploaded the blob
   * @param containerId containerId of the blob
   * @param version the version in which {@link BlobProperties} needs to be created
   * @return the {@link BlobProperties} thus created
   */
  private BlobProperties getBlobProperties(long blobSize, boolean isEncrypted, String serviceId, String ownerId,
      String contentType, boolean isPrivate, long timeToLiveInSeconds, short accountId, short containerId,
      short version) {
    if (version == BlobPropertiesSerDe.VERSION_1) {
      return new BlobProperties(blobSize, serviceId, ownerId, contentType, isPrivate, timeToLiveInSeconds,
          Account.UNKNOWN_ACCOUNT_ID, Container.UNKNOWN_CONTAINER_ID, isEncrypted);
    } else {
      return new BlobProperties(blobSize, serviceId, ownerId, contentType, isPrivate, timeToLiveInSeconds, accountId,
          containerId, isEncrypted);
    }
  }

  /**
   * Creates {@link BlobProperties} based on the args passed for the given version
   * @param blobSize the size of the blob
   * @param isEncrypted whether the blob is encrypted
   * @param serviceId the serviceId associated with the {@link BlobProperties}
   * @param accountId accountId of the user who uploaded the blob
   * @param containerId containerId of the blob
   * @param version the version in which {@link BlobProperties} needs to be created
   * @return the {@link BlobProperties} thus created
   */
  private BlobProperties getBlobProperties(long blobSize, boolean isEncrypted, String serviceId, short accountId,
      short containerId, short version) {
    if (version == BlobPropertiesSerDe.VERSION_1) {
      return new BlobProperties(blobSize, serviceId, Account.UNKNOWN_ACCOUNT_ID, Container.UNKNOWN_CONTAINER_ID, false);
    } else if (version == BlobPropertiesSerDe.VERSION_2) {
      return new BlobProperties(blobSize, serviceId, accountId, containerId, false);
    } else {
      return new BlobProperties(blobSize, serviceId, null, null, false, Utils.Infinite_Time,
          SystemTime.getInstance().milliseconds(), accountId, containerId, isEncrypted);
    }
  }

  /**
   * Creates BlobPropertiesSerDe in V1 and returns the deserialized version of {@link BlobProperties}
   * @param blobProperties {@link BlobProperties} that needs to be serialized in V1
   * @return {@link ByteBuffer} representing the serialized form of {@link BlobProperties} in V1
   */
  private ByteBuffer getBlobPropsSerDeV1(BlobProperties blobProperties) {
    int size = Version_Field_Size_In_Bytes + Long.BYTES + Byte.BYTES + Long.BYTES + Long.BYTES + Integer.BYTES
        + Utils.getNullableStringLength(blobProperties.getContentType()) + Integer.BYTES
        + Utils.getNullableStringLength(blobProperties.getOwnerId()) + Integer.BYTES + Utils.getNullableStringLength(
        blobProperties.getServiceId());
    ByteBuffer outputBuffer = ByteBuffer.allocate(size);
    outputBuffer.putShort(VERSION_1);
    outputBuffer.putLong(blobProperties.getTimeToLiveInSeconds());
    outputBuffer.put(blobProperties.isPrivate() ? (byte) 1 : (byte) 0);
    outputBuffer.putLong(blobProperties.getCreationTimeInMs());
    outputBuffer.putLong(blobProperties.getBlobSize());
    Utils.serializeNullableString(outputBuffer, blobProperties.getContentType());
    Utils.serializeNullableString(outputBuffer, blobProperties.getOwnerId());
    Utils.serializeNullableString(outputBuffer, blobProperties.getServiceId());
    return outputBuffer;
  }

  /**
   * Creates BlobPropertiesSerDe in V2 and returns the deserialized version of {@link BlobProperties}
   * @param blobProperties {@link BlobProperties} that needs to be serialized in V1
   * @return {@link ByteBuffer} representing the serialized form of {@link BlobProperties} in V1
   */
  private ByteBuffer getBlobPropsSerDeV2(BlobProperties blobProperties) {
    int size = Version_Field_Size_In_Bytes + Long.BYTES + Byte.BYTES + Long.BYTES + Long.BYTES + Integer.BYTES
        + Utils.getNullableStringLength(blobProperties.getContentType()) + Integer.BYTES
        + Utils.getNullableStringLength(blobProperties.getOwnerId()) + Integer.BYTES + Utils.getNullableStringLength(
        blobProperties.getServiceId()) + Short.BYTES + Short.BYTES;
    ByteBuffer outputBuffer = ByteBuffer.allocate(size);
    outputBuffer.putShort(VERSION_2);
    outputBuffer.putLong(blobProperties.getTimeToLiveInSeconds());
    outputBuffer.put(blobProperties.isPrivate() ? (byte) 1 : (byte) 0);
    outputBuffer.putLong(blobProperties.getCreationTimeInMs());
    outputBuffer.putLong(blobProperties.getBlobSize());
    Utils.serializeNullableString(outputBuffer, blobProperties.getContentType());
    Utils.serializeNullableString(outputBuffer, blobProperties.getOwnerId());
    Utils.serializeNullableString(outputBuffer, blobProperties.getServiceId());
    outputBuffer.putShort(blobProperties.getAccountId());
    outputBuffer.putShort(blobProperties.getContainerId());
    return outputBuffer;
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
   * @param isEncrypted whether the blob is encrypted.
   */
  private void verifyBlobProperties(BlobProperties blobProperties, long blobSize, String serviceId, String ownerId,
      String contentType, boolean isPrivate, long ttlInSecs, short accountId, short containerId, boolean isEncrypted) {
    assertEquals(blobProperties.getBlobSize(), blobSize);
    assertEquals(blobProperties.getServiceId(), serviceId);
    assertEquals(blobProperties.getOwnerId(), ownerId);
    assertEquals(blobProperties.getContentType(), contentType);
    assertEquals(blobProperties.isPrivate(), isPrivate);
    assertEquals(blobProperties.getTimeToLiveInSeconds(), ttlInSecs);
    assertEquals("AccountId mismatch ", accountId, blobProperties.getAccountId());
    assertEquals("ContainerId mismatch ", containerId, blobProperties.getContainerId());
    assertEquals(isEncrypted, blobProperties.isEncrypted());
  }
}
