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
package com.github.ambry.commons;

import com.github.ambry.account.Account;
import com.github.ambry.account.Container;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.Datacenter;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.utils.ByteBufferInputStream;
import java.io.DataInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.commons.codec.binary.Base64;
import org.junit.Before;
import org.junit.Test;

import static com.github.ambry.commons.BlobId.*;
import static org.junit.Assert.*;


/**
 * Unit tests for {@link BlobId} and {@link BlobIdBuilder}.
 */
public class BlobIdTest {
  private byte referenceFlag = 127;
  private byte referenceDatacenterId = 8;
  private short referenceAccountId = 7777;
  private short referenceContainerId = 8888;
  private ClusterMap referenceClusterMap;
  private PartitionId referencePartitionId;
  private List<Short> versions = Arrays.asList(new Short[]{BLOB_ID_V1, BLOB_ID_V2});

  /**
   * Initialization before each unit test.
   * @throws Exception Any unexpected exception.
   */
  @Before
  public void init() throws Exception {
    referenceClusterMap = new MockClusterMap();
    referencePartitionId = referenceClusterMap.getWritablePartitionIds().get(0);
  }

  /**
   * Tests building BlobId in version 1.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testBuildBlobIdV1() throws Exception {
    // use BlobIdBuilder(partitionId)
    BlobId blobId = new BlobIdBuilder(referencePartitionId).build();
    assertEquals("Wrong serialized version", BLOB_ID_V1, getVersionFromBlobString(blobId.toString()));
    assertBlob(blobId, BlobId.DEFAULT_FLAG, Datacenter.LEGACY_DATACENTER_ID, Account.LEGACY_ACCOUNT_ID,
        Container.LEGACY_CONTAINER_ID, referencePartitionId);

    // use BlobIdBuilder(flag, datacenterId, accountId, containerId, partitionId), where the first four are null;
    buildBlobIdAndAssert(null, null, null, null, referencePartitionId, BLOB_ID_V1);
  }

  /**
   * Tests building BlobId in version 2. This will validate that even a single non-null field of either of
   * {@code flag}, {@code datacenterId}, {@code accountId}, or {@code containerId} will create blobId version 2.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testBuildBlobIdV2() throws Exception {
    buildBlobIdAndAssert(referenceFlag, referenceDatacenterId, referenceAccountId, referenceContainerId,
        referencePartitionId, BLOB_ID_V2);
    buildBlobIdAndAssert(null, referenceDatacenterId, referenceAccountId, referenceContainerId, referencePartitionId,
        BLOB_ID_V2);
    buildBlobIdAndAssert(referenceFlag, null, referenceAccountId, referenceContainerId, referencePartitionId,
        BLOB_ID_V2);
    buildBlobIdAndAssert(referenceFlag, referenceDatacenterId, null, referenceContainerId, referencePartitionId,
        BLOB_ID_V2);
    buildBlobIdAndAssert(referenceFlag, referenceDatacenterId, referenceAccountId, null, referencePartitionId,
        BLOB_ID_V2);
  }

  /**
   * Tests setter in {@link BlobIdBuilder}.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testSetAndBuild() throws Exception {
    Byte flag = (byte) (referenceFlag + 1);
    Byte datacenterId = (byte) (referenceDatacenterId + 1);
    Short accountId = (short) (referenceAccountId + 1);
    Short containerId = (short) (referenceContainerId + 1);
    PartitionId partitionId = referenceClusterMap.getWritablePartitionIds().get(1);
    BlobIdBuilder blobIdBuilder =
        new BlobIdBuilder(referenceFlag, referenceDatacenterId, referenceAccountId, referenceContainerId,
            referencePartitionId);
    BlobId blobId = blobIdBuilder.setFlag(flag)
        .setDatacenterId(datacenterId)
        .setAccountId(accountId)
        .setContainerId(containerId)
        .setPartitionId(partitionId)
        .build();
    assertBlob(blobId, flag, datacenterId, accountId, containerId, partitionId);
  }

  /**
   * Tests when not only {@code partitionId} is available, it will create version 2 of blobId.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testWhenOnlyPartitionIdAvailable() throws Exception {
    BlobId idV2 = new BlobIdBuilder(referencePartitionId).setAccountId(referenceAccountId).build();
    assertEquals("Wrong serialized version", BLOB_ID_V2, getVersionFromBlobString(idV2.toString()));
    assertBlob(idV2, null, null, referenceAccountId, null, referencePartitionId);
  }

  /**
   * Tests deserialization from a composed blob id string (not from deserialization of a blobId in both version
   * 1 and 2.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testDeserialization() throws Exception {
    for (Short version : versions) {
      composedBlobIdAndDeserialize(version);
    }
  }

  /**
   * Tests first serializing a blob id into string, and then deserializing into a blobid object from the string.
   * The tests are performed on both blob v1 and v2.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testSerDes() throws Exception {
    for (Short version : versions) {
      serDesBlobId(version);
    }
  }

  /**
   * Test various invalid blob IDs
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void badIdTest() throws Exception {
    for (Short version : versions) {
      generateAndAssertBadBlobId(version);
    }
  }

  /**
   * Tests blobId long form in both v1 and v2.
   */
  @Test
  public void testBlobIdLongForm() {
    assertBlobIdLongForm(BLOB_ID_V1);
    assertBlobIdLongForm(BLOB_ID_V2);
  }

  /**
   * Makes a blobId-like string in the same way as used in blobId serialization, and then deserialize from the
   * string to construct blobId object.
   * @param version The version of BlobId.
   * @throws Exception Any unexpected exception.
   */
  private void composedBlobIdAndDeserialize(short version) throws Exception {
    String srcUUID = UUID.randomUUID().toString();
    String srcBlobIdStr;
    switch (version) {
      case BLOB_ID_V1:
        srcBlobIdStr =
            buildBlobIdLike(version, null, null, null, null, referencePartitionId, srcUUID.length(), srcUUID);
        break;

      case BLOB_ID_V2:
        srcBlobIdStr =
            buildBlobIdLike(version, referenceFlag, referenceDatacenterId, referenceAccountId, referenceContainerId,
                referencePartitionId, srcUUID.length(), srcUUID);
        break;

      default:
        throw new IllegalArgumentException("Invalid version number blob" + version);
    }
    deserializeBlobIdAndAssert(version, srcBlobIdStr, referencePartitionId);
  }

  /**
   * Construct a blobId, serialize it into string, and then deserialize from the string to construct BlobId object.
   * @param version The version of BlobId.
   * @throws Exception Any unexpected exception.
   */
  private void serDesBlobId(short version) throws Exception {
    String srcBlobIdStr;
    switch (version) {
      case BLOB_ID_V1:
        srcBlobIdStr = new BlobIdBuilder(referencePartitionId).build().getID();
        break;
      case BLOB_ID_V2:
        srcBlobIdStr = new BlobIdBuilder(referenceFlag, referenceDatacenterId, referenceAccountId, referenceContainerId,
            referencePartitionId).build().getID();
        break;
      default:
        throw new IllegalArgumentException("invalid version number blob" + version);
    }
    deserializeBlobIdAndAssert(version, srcBlobIdStr, referencePartitionId);
  }

  /**
   * Generates bad blobId strings, and deserializes from the string.
   * @param version The version of BlobId.
   * @throws Exception Any unexpected exception.
   */
  private void generateAndAssertBadBlobId(Short version) throws Exception {
    List<String> blobIdLikes = new ArrayList<>();
    PartitionId badPartitionId = new MockPartitionId(200000, Collections.EMPTY_LIST, 0);
    String goodUUID = UUID.randomUUID().toString();

    // Partition ID not in cluster map
    blobIdLikes.add(
        buildBlobIdLike(version, referenceFlag, referenceDatacenterId, referenceAccountId, referenceContainerId,
            badPartitionId, goodUUID.length(), goodUUID));
    // UUID length too long
    blobIdLikes.add(
        buildBlobIdLike(version, referenceFlag, referenceDatacenterId, referenceAccountId, referenceContainerId,
            referencePartitionId, goodUUID.length() + 1, goodUUID));
    // UUID length too short
    blobIdLikes.add(
        buildBlobIdLike(version, referenceFlag, referenceDatacenterId, referenceAccountId, referenceContainerId,
            referencePartitionId, goodUUID.length() - 1, goodUUID));
    // UUID length is negative
    blobIdLikes.add(
        buildBlobIdLike(version, referenceFlag, referenceDatacenterId, referenceAccountId, referenceContainerId,
            referencePartitionId, -1, goodUUID));
    // Extra characters after UUID
    blobIdLikes.add(
        buildBlobIdLike(version, referenceFlag, referenceDatacenterId, referenceAccountId, referenceContainerId,
            referencePartitionId, goodUUID.length(), goodUUID + "EXTRA"));
    // Invalid version number
    blobIdLikes.add(buildBlobIdLike((short) (BLOB_ID_V2 + 1), referenceFlag, referenceDatacenterId, referenceAccountId,
        referenceContainerId, referencePartitionId, goodUUID.length(), goodUUID));
    // Empty blob ID
    blobIdLikes.add("");
    // short Blob ID
    blobIdLikes.add("AA");

    for (String blobIdLike : blobIdLikes) {
      try {
        BlobId.fromStringId(blobIdLike, referenceClusterMap);
        fail("Expected blob ID creation to fail with blob ID string " + blobIdLike);
      } catch (Exception e) {
        // expected
      }
    }
  }

  /**
   * Build a string that resembles a blob ID, but with certain fields possibly set to legacy values.
   * @param version The version number to be embedded in the blob id.
   * @param flag The flag to be embedded in the blob id.
   * @param datacenterId The datacenter id to be embedded in the blob id.
   * @param accountId The account id to be embedded in the blob id.
   * @param containerId The container id to be embedded in the blob id.
   * @param partitionId The partition id to be embedded in the blob id.
   * @param uuidLength The length of the uuid.
   * @param uuidLike The UUID to be embedded in the blob id.
   * @return a base-64 encoded {@link String} representing the blob id.
   */
  private String buildBlobIdLike(short version, Byte flag, Byte datacenterId, Short accountId, Short containerId,
      PartitionId partitionId, int uuidLength, String uuidLike) {
    int idLength;
    ByteBuffer idBuf;
    switch (version) {
      case BLOB_ID_V1:
        idLength = 2 + partitionId.getBytes().length + 4 + uuidLike.length();
        idBuf = ByteBuffer.allocate(idLength);
        idBuf.putShort(version);
        break;

      case BLOB_ID_V2:
        idLength = 2 + 1 + 1 + 2 + 2 + partitionId.getBytes().length + 4 + uuidLike.length();
        idBuf = ByteBuffer.allocate(idLength);
        idBuf.putShort(version);
        idBuf.put(flag);
        idBuf.put(datacenterId);
        idBuf.putShort(accountId);
        idBuf.putShort(containerId);
        break;

      default:
        idLength = 2 + partitionId.getBytes().length + 4 + uuidLike.length();
        idBuf = ByteBuffer.allocate(idLength);
        idBuf.putShort(version);
        break;
    }
    idBuf.put(partitionId.getBytes());
    idBuf.putInt(uuidLength);
    idBuf.put(uuidLike.getBytes());
    return Base64.encodeBase64URLSafeString(idBuf.array());
  }

  /**
   * Deserializes BlobId string and assert the resulted BlobId object.
   * @param version The version of BlobId.
   * @param srcBlobIdStr The string to deserialize.
   * @param referencePartitionId The reference PartitionId.
   * @throws Exception Any unexpected exception.
   */
  private void deserializeBlobIdAndAssert(short version, String srcBlobIdStr, PartitionId referencePartitionId)
      throws Exception {
    List<BlobId> blobIds = new ArrayList<>();
    blobIds.add(BlobId.fromStringId(srcBlobIdStr, referenceClusterMap));
    blobIds.add(BlobId.fromDataInputStream(getStreamFromBase64(srcBlobIdStr), referenceClusterMap));
    blobIds.add(BlobId.fromDataInputStream(getStreamFromBase64(srcBlobIdStr + "EXTRA"), referenceClusterMap));
    for (BlobId blobId : blobIds) {
      assertEquals("Wrong partition ID in blob ID: " + blobId, referencePartitionId, blobId.getPartition());
      assertEquals("Wrong base-64 ID in blob ID: " + blobId, srcBlobIdStr, blobId.getID());
      switch (version) {
        case BLOB_ID_V1:
          assertEquals("Wrong flag in blob ID: " + blobId, BlobId.DEFAULT_FLAG, blobId.getFlag());
          assertEquals("Wrong datacenter ID in blob ID: " + blobId, Datacenter.LEGACY_DATACENTER_ID,
              blobId.getDatacenterId());
          assertEquals("Wrong account ID in blob ID: " + blobId, Account.LEGACY_ACCOUNT_ID, blobId.getAccountId());
          assertEquals("Wrong container ID in blob ID: " + blobId, Container.LEGACY_CONTAINER_ID,
              blobId.getContainerId());
          break;

        case BLOB_ID_V2:
          assertEquals("Wrong flag in blob ID: " + blobId, referenceFlag, blobId.getFlag());
          assertEquals("Wrong datacenter ID in blob ID: " + blobId, referenceDatacenterId, blobId.getDatacenterId());
          assertEquals("Wrong account ID in blob ID: " + blobId, referenceAccountId, blobId.getAccountId());
          assertEquals("Wrong container ID in blob ID: " + blobId, referenceContainerId, blobId.getContainerId());
          break;

        default:
          throw new IllegalArgumentException("invalid version number blob" + version);
      }
    }
  }

  /**
   * Convert a base-64 encoded string into a {@link DataInputStream}
   * @param base64String the base-64 encoded {@link String}
   * @return the {@link DataInputStream}
   */
  private DataInputStream getStreamFromBase64(String base64String) {
    return new DataInputStream(new ByteBufferInputStream(ByteBuffer.wrap(Base64.decodeBase64(base64String))));
  }

  /**
   * Asserts a {@link BlobId} against the expected values.
   * @param blobId The {@link BlobId} to assert.
   * @param flag The expected {@code flag}. If {@code null}, the assertion will be run against
   * {@link BlobId#DEFAULT_FLAG}.
   * @param datacenterId The expected {@code datacenterId}. If {@code null}, the assertion will be run against
   * {@link Datacenter#LEGACY_DATACENTER_ID}.
   * @param accountId The expected {@code accountId}. If {@code null}, the assertion will be run against
   * {@link Account#LEGACY_ACCOUNT_ID}.
   * @param containerId The expected {@code containerId}. If {@code null}, the assertion will be run against
   * {@link Container#LEGACY_CONTAINER_ID}.
   * @param partitionId The expected partitionId.
   * @throws Exception Any unexpected exception.
   */
  private void assertBlob(BlobId blobId, Byte flag, Byte datacenterId, Short accountId, Short containerId,
      PartitionId partitionId) throws Exception {
    assertEquals("Wrong flag in blob ID: " + blobId, flag == null ? BlobId.DEFAULT_FLAG : flag, blobId.getFlag());
    assertEquals("Wrong datacenter id in blob ID: " + blobId,
        datacenterId == null ? Datacenter.LEGACY_DATACENTER_ID : datacenterId, blobId.getDatacenterId());
    assertEquals("Wrong account id in blob ID: " + blobId, accountId == null ? Account.LEGACY_ACCOUNT_ID : accountId,
        blobId.getAccountId());
    assertEquals("Wrong container id in blob ID: " + blobId,
        containerId == null ? Container.LEGACY_CONTAINER_ID : containerId, blobId.getContainerId());
    assertEquals("Wrong partition id in blob ID: " + blobId, partitionId, blobId.getPartition());
    assertEquals("Wrong blob after serDes.", blobId, BlobId.fromStringId(blobId.getID(), referenceClusterMap));
    System.out.println("BlobId=" + blobId + ", flag=" + blobId.getFlag() + ", datacenterId=" + blobId.getDatacenterId()
        + ", accountId=" + blobId.getAccountId() + ", containerId=" + blobId.getContainerId() + ", idSizeInBytes="
        + blobId.toString().length());
  }

  /**
   * Builds a {@link BlobId} and assert it against the expected values.
   * @param flag The expected {@code flag}. If {@code null}, the assertion will be run against
   * {@link BlobId#DEFAULT_FLAG}.
   * @param datacenterId The expected {@code datacenterId}. If {@code null}, the assertion will be run against
   * {@link Datacenter#LEGACY_DATACENTER_ID}.
   * @param accountId The expected {@code accountId}. If {@code null}, the assertion will be run against
   * {@link Account#LEGACY_ACCOUNT_ID}.
   * @param containerId The expected {@code containerId}. If {@code null}, the assertion will be run against
   * {@link Container#LEGACY_CONTAINER_ID}.
   * @param partitionId The expected partitionId.
   * @param expectedBlobVersion The expected version number.
   * @throws Exception Any unexpected exception.
   */
  private void buildBlobIdAndAssert(Byte flag, Byte datacenterId, Short accountId, Short containerId,
      PartitionId partitionId, short expectedBlobVersion) throws Exception {
    BlobId blobId = new BlobIdBuilder(flag, datacenterId, accountId, containerId, partitionId).build();
    assertEquals("Wrong blob id version", expectedBlobVersion, getVersionFromBlobString(blobId.getID()));
    assertBlob(blobId, flag, datacenterId, accountId, containerId, partitionId);
  }

  /**
   * Gets the version number from a blobId string.
   * @param blobId The blobId string to get version number.
   * @return Version number
   * @throws Exception Any unexpected exception.
   */
  private short getVersionFromBlobString(String blobId) throws Exception {
    DataInputStream dis = new DataInputStream(new ByteBufferInputStream(ByteBuffer.wrap(Base64.decodeBase64(blobId))));
    try {
      return dis.readShort();
    } finally {
      dis.close();
    }
  }

  /**
   * Asserts blobId long form.
   * @param version The version number of blobId.
   */
  private void assertBlobIdLongForm(short version) {
    BlobId blobId;
    switch (version) {
      case BLOB_ID_V1:
        blobId = new BlobIdBuilder(referencePartitionId).build();
        break;

      case BLOB_ID_V2:
        blobId = new BlobIdBuilder(referenceFlag, referenceDatacenterId, referenceAccountId, referenceContainerId,
            referencePartitionId).build();
        break;

      default:
        fail("Unrecognized blob version number");
        return;
    }
    String blobIdStr = blobId.getID();
    String blobIdLongForm = blobId.getLongForm();
    String blobLongFormWithoutUuid = blobIdLongForm.substring(0, blobIdLongForm.lastIndexOf(':'));
    StringBuilder expectedBlobIdLongFormWithoutUuidSb =
        new StringBuilder().append("[").append(blobIdStr).append(":").append(version).append(":");
    if (version == BLOB_ID_V2) {
      expectedBlobIdLongFormWithoutUuidSb.append(referenceFlag)
          .append(":")
          .append(referenceDatacenterId)
          .append(":")
          .append(referenceAccountId)
          .append(":")
          .append(referenceContainerId)
          .append(":");
    }
    String expectedBlobIdLongFormWithoutUuidStr =
        expectedBlobIdLongFormWithoutUuidSb.append(referencePartitionId).toString();
    assertEquals("Wrong blob id long form.", expectedBlobIdLongFormWithoutUuidStr, blobLongFormWithoutUuid);
  }
}
