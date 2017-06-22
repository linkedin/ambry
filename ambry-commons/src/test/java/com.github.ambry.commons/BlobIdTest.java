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
import java.util.Random;
import java.util.UUID;
import org.apache.commons.codec.binary.Base64;
import org.junit.Before;
import org.junit.Test;

import static com.github.ambry.clustermap.ClusterMapUtils.*;
import static com.github.ambry.commons.BlobId.*;
import static com.github.ambry.utils.Utils.*;
import static org.junit.Assert.*;


/**
 * Unit tests for {@link BlobId}. Using {@link BlobIdV2} builds a {@link BlobId}
 * in v2 equivalent to setting {@link BlobId#CURRENT_VERSION} to be {@link BlobId#BLOB_ID_V2}.
 */
public class BlobIdTest {
  private static final Random random = new Random();
  private static final List<Short> versions = Arrays.asList(new Short[]{BLOB_ID_V1, BLOB_ID_V2});
  private byte referenceFlag;
  private byte referenceDatacenterId;
  private short referenceAccountId;
  private short referenceContainerId;
  private ClusterMap referenceClusterMap;
  private PartitionId referencePartitionId;

  /**
   * Initialization before each unit test.
   * @throws Exception Any unexpected exception.
   */
  @Before
  public void init() throws Exception {
    byte[] bytes = new byte[2];
    referenceClusterMap = new MockClusterMap();
    random.nextBytes(bytes);
    referenceFlag = bytes[0];
    referenceDatacenterId = bytes[0];
    referenceAccountId = getRandomShort(random);
    referenceContainerId = getRandomShort(random);
    referencePartitionId = referenceClusterMap.getWritablePartitionIds().get(0);
  }

  /**
   * Tests building blobId in both {@link BlobId#BLOB_ID_V1} and {@link BlobId#BLOB_ID_V2}. The expected values
   * for {@code flag}, {@code datacenterId}, {@code accountId}, {@code containerId}, and {@code partitionId} are
   * listed below:
   * <pre>
   * Version
   * 1            always default values except partitionId
   * 2            values passed in as arguments
   * </pre>
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testBuildBlobId() throws Exception {
    for (Short version : versions) {
      buildBlobIdAndAssert(version, referenceFlag, referenceDatacenterId, referenceAccountId, referenceContainerId,
          referencePartitionId);
    }
  }

  /**
   * Tests deserialization from a composed blobId string (not from deserialization of a blobId in both version
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
   * Tests first serializing a blobId into string, and then deserializing into a blobId object from the string.
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
   * Test various invalid blobIds
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
    for (Short version : versions) {
      assertBlobIdLongForm(version);
    }
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
    srcBlobIdStr =
        buildBlobIdLike(version, referenceFlag, referenceDatacenterId, referenceAccountId, referenceContainerId,
            referencePartitionId, srcUUID.length(), srcUUID);
    deserializeBlobIdAndAssert(version, srcBlobIdStr);
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
        srcBlobIdStr = new BlobId(referenceFlag, referenceDatacenterId, referenceAccountId, referenceContainerId,
            referencePartitionId).getID();
        break;

      case BLOB_ID_V2:
        srcBlobIdStr = new BlobIdV2(referenceFlag, referenceDatacenterId, referenceAccountId, referenceContainerId,
            referencePartitionId).getID();
        break;

      default:
        throw new IllegalArgumentException("invalid version number blob" + version);
    }
    deserializeBlobIdAndAssert(version, srcBlobIdStr);
  }

  /**
   * Generates bad blobId strings, and deserializes from the string.
   * @param version The version of BlobId.
   * @throws Exception Any unexpected exception.
   */
  private void generateAndAssertBadBlobId(Short version) throws Exception {
    List<String> invalidBlobIdLikeList = new ArrayList<>();
    PartitionId badPartitionId = new MockPartitionId(200000, Collections.EMPTY_LIST, 0);
    String goodUUID = UUID.randomUUID().toString();

    // Partition ID not in cluster map
    invalidBlobIdLikeList.add(
        buildBlobIdLike(version, referenceFlag, referenceDatacenterId, referenceAccountId, referenceContainerId,
            badPartitionId, goodUUID.length(), goodUUID));
    // UUID length too long
    invalidBlobIdLikeList.add(
        buildBlobIdLike(version, referenceFlag, referenceDatacenterId, referenceAccountId, referenceContainerId,
            referencePartitionId, goodUUID.length() + 1, goodUUID));
    // UUID length too short
    invalidBlobIdLikeList.add(
        buildBlobIdLike(version, referenceFlag, referenceDatacenterId, referenceAccountId, referenceContainerId,
            referencePartitionId, goodUUID.length() - 1, goodUUID));
    // UUID length is negative
    invalidBlobIdLikeList.add(
        buildBlobIdLike(version, referenceFlag, referenceDatacenterId, referenceAccountId, referenceContainerId,
            referencePartitionId, -1, goodUUID));
    // Extra characters after UUID
    invalidBlobIdLikeList.add(
        buildBlobIdLike(version, referenceFlag, referenceDatacenterId, referenceAccountId, referenceContainerId,
            referencePartitionId, goodUUID.length(), goodUUID + "EXTRA"));
    // Invalid version number
    invalidBlobIdLikeList.add(
        buildBlobIdLike((short) (BLOB_ID_V2 + 1), referenceFlag, referenceDatacenterId, referenceAccountId,
            referenceContainerId, referencePartitionId, goodUUID.length(), goodUUID));
    // Empty blobId
    invalidBlobIdLikeList.add("");
    // short Blob ID
    invalidBlobIdLikeList.add("AA");

    for (String blobIdLike : invalidBlobIdLikeList) {
      try {
        BlobId.fromStringId(blobIdLike, referenceClusterMap);
        fail("Expected blobId creation to fail with blobId string " + blobIdLike);
      } catch (Exception e) {
        // expected
      }
    }
  }

  /**
   * Build a string that resembles a blobId, but with certain fields possibly set to legacy values.
   * @param version The version number to be embedded in the blobId.
   * @param flag The flag to be embedded in the blobId.
   * @param datacenterId The datacenter id to be embedded in the blobId.
   * @param accountId The account id to be embedded in the blobId.
   * @param containerId The container id to be embedded in the blobId.
   * @param partitionId The partition id to be embedded in the blobId.
   * @param uuidLength The length of the uuid.
   * @param uuidLike The UUID to be embedded in the blobId.
   * @return a base-64 encoded {@link String} representing the blobId.
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
   * @throws Exception Any unexpected exception.
   */
  private void deserializeBlobIdAndAssert(short version, String srcBlobIdStr) throws Exception {
    List<BlobId> blobIds = new ArrayList<>();
    blobIds.add(BlobId.fromStringId(srcBlobIdStr, referenceClusterMap));
    blobIds.add(BlobId.fromDataInputStream(getStreamFromBase64(srcBlobIdStr), referenceClusterMap));
    blobIds.add(BlobId.fromDataInputStream(getStreamFromBase64(srcBlobIdStr + "EXTRA"), referenceClusterMap));
    for (BlobId blobId : blobIds) {
      assertEquals("Wrong partition ID in blobId: " + blobId, referencePartitionId, blobId.getPartition());
      assertEquals("Wrong base-64 ID in blobId: " + blobId, srcBlobIdStr, blobId.getID());
      assertEquals("Wrong blobId version", version, getVersionFromBlobString(blobId.getID()));
      switch (version) {
        case BLOB_ID_V1:
          assertEquals("Wrong flag in blobId: " + blobId, BlobId.DEFAULT_FLAG, blobId.getFlag());
          assertEquals("Wrong datacenter ID in blobId: " + blobId, LEGACY_DATACENTER_ID, blobId.getDatacenterId());
          assertEquals("Wrong account ID in blobId: " + blobId, Account.LEGACY_ACCOUNT_ID, blobId.getAccountId());
          assertEquals("Wrong container ID in blobId: " + blobId, Container.LEGACY_CONTAINER_ID,
              blobId.getContainerId());
          break;

        case BLOB_ID_V2:
          assertEquals("Wrong flag in blobId: " + blobId, referenceFlag, blobId.getFlag());
          assertEquals("Wrong datacenter ID in blobId: " + blobId, referenceDatacenterId, blobId.getDatacenterId());
          assertEquals("Wrong account ID in blobId: " + blobId, referenceAccountId, blobId.getAccountId());
          assertEquals("Wrong container ID in blobId: " + blobId, referenceContainerId, blobId.getContainerId());
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
   * Builds a {@link BlobId} using the arguments and asserts against the argument values.
   * @param version The version of blobId to build.
   * @param flag The expected {@code flag}. If {@code null}, the assertion will be run against
   * {@link BlobId#DEFAULT_FLAG}.
   * @param datacenterId The expected {@code datacenterId}. If {@code null}, the assertion will be run against
   * {@link com.github.ambry.clustermap.ClusterMapUtils#LEGACY_DATACENTER_ID}.
   * @param accountId The expected {@code accountId}. If {@code null}, the assertion will be run against
   * {@link Account#LEGACY_ACCOUNT_ID}.
   * @param containerId The expected {@code containerId}. If {@code null}, the assertion will be run against
   * {@link Container#LEGACY_CONTAINER_ID}.
   * @param partitionId The expected partitionId.
   * @throws Exception Any unexpected exception.
   */
  private void buildBlobIdAndAssert(short version, byte flag, byte datacenterId, short accountId, short containerId,
      PartitionId partitionId) throws Exception {
    BlobId blobId;
    switch (version) {
      case BLOB_ID_V1:
        blobId = new BlobId(flag, datacenterId, accountId, containerId, partitionId);
        break;

      case BLOB_ID_V2:
        blobId = new BlobIdV2(flag, datacenterId, accountId, containerId, partitionId);
        break;

      default:
        throw new IllegalArgumentException("Invalid version number blob" + version);
    }
    assertEquals("Wrong blobId version", version, getVersionFromBlobString(blobId.getID()));
    assertBlobIdFieldValues(version, blobId, flag, datacenterId, accountId, containerId, partitionId);
  }

  /**
   * Asserts a {@link BlobId} against the expected values.
   * @param version The expected version of the blobId.
   * @param blobId The {@link BlobId} to assert.
   * @param flag The expected {@code flag}. This will be of no effect if version is set to v1, and the expected value will
   *             become {@link BlobId#DEFAULT_FLAG}. For v2, {@code null} will make the assertion against {@link BlobId#DEFAULT_FLAG}.
   * @param datacenterId The expected {@code datacenterId}. This will be of no effect if version is set to v1, and the
   *                     expected value will become {@link com.github.ambry.clustermap.ClusterMapUtils#LEGACY_DATACENTER_ID}.
   *                     For v2, {@code null} will make the assertion against
   *                     {@link com.github.ambry.clustermap.ClusterMapUtils#LEGACY_DATACENTER_ID}.
   * @param accountId The expected {@code accountId}. This will be of no effect if version is set to v1, and the expected
   *                  value will become {@link Account#LEGACY_ACCOUNT_ID}. For v2, {@code null} will make the assertion
   *                  against {@link Account#LEGACY_ACCOUNT_ID}.
   * @param containerId The expected {@code containerId}. This will be of no effect if version is set to v1, and the
   *                    expected value will become {@link Container#LEGACY_CONTAINER_ID}. For v2, {@code null} will make
   *                    the assertion against {@link Container#LEGACY_CONTAINER_ID}.
   * @param partitionId The expected partitionId.
   * @throws Exception Any unexpected exception.
   */
  private void assertBlobIdFieldValues(short version, BlobId blobId, Byte flag, Byte datacenterId, Short accountId,
      Short containerId, PartitionId partitionId) throws Exception {
    assertTrue("Used unrecognized version", version == BLOB_ID_V1 || version == BLOB_ID_V2);
    assertEquals("Wrong partition id in blobId: " + blobId, partitionId, blobId.getPartition());
    assertEquals("Wrong version in blobId.", version, blobId.getCurrentVersion());
    if (version == BLOB_ID_V1) {
      assertEquals("Wrong flag in blobId: " + blobId, BlobId.DEFAULT_FLAG, blobId.getFlag());
      assertEquals("Wrong datacenter id in blobId: " + blobId, LEGACY_DATACENTER_ID, blobId.getDatacenterId());
      assertEquals("Wrong account id in blobId: " + blobId, Account.LEGACY_ACCOUNT_ID, blobId.getAccountId());
      assertEquals("Wrong container id in blobId: " + blobId, Container.LEGACY_CONTAINER_ID, blobId.getContainerId());
    } else {
      assertEquals("Wrong flag in blobId: " + blobId, flag == null ? BlobId.DEFAULT_FLAG : flag, blobId.getFlag());
      assertEquals("Wrong datacenter id in blobId: " + blobId,
          datacenterId == null ? LEGACY_DATACENTER_ID : datacenterId, blobId.getDatacenterId());
      assertEquals("Wrong account id in blobId: " + blobId, accountId == null ? Account.LEGACY_ACCOUNT_ID : accountId,
          blobId.getAccountId());
      assertEquals("Wrong container id in blobId: " + blobId,
          containerId == null ? Container.LEGACY_CONTAINER_ID : containerId, blobId.getContainerId());
    }
    System.out.println("BlobId=" + blobId + ", flag=" + blobId.getFlag() + ", datacenterId=" + blobId.getDatacenterId()
        + ", accountId=" + blobId.getAccountId() + ", containerId=" + blobId.getContainerId() + ", idSizeInBytes="
        + blobId.toString().length());
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
        blobId = new BlobId(referenceFlag, referenceDatacenterId, referenceAccountId, referenceContainerId,
            referencePartitionId);
        break;

      case BLOB_ID_V2:
        blobId = new BlobIdV2(referenceFlag, referenceDatacenterId, referenceAccountId, referenceContainerId,
            referencePartitionId);
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
    assertEquals("Wrong blobId long form.", expectedBlobIdLongFormWithoutUuidStr, blobLongFormWithoutUuid);
  }

  /**
   * A class that is used to test when {@link BlobId#CURRENT_VERSION} is set to {@link BlobId#BLOB_ID_V2}.
   */
  private class BlobIdV2 extends BlobId {
    BlobIdV2(byte flag, byte datacenterId, short accountId, short containerId, PartitionId partitionId) {
      super(flag, datacenterId, accountId, containerId, partitionId);
    }

    @Override
    short getCurrentVersion() {
      return BLOB_ID_V2;
    }
  }
}
