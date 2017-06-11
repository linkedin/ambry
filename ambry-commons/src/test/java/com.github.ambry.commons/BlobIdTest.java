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
 * Unit tests for {@link BlobId} and {@link BlobIdBuilder}. Using {@link BlobIdBuilderV2} builds a {@link BlobId}
 * in v2 equivalent to setting {@link BlobId#CURRENT_VERSION} to be {@link BlobId#BLOB_ID_V2}.
 */
public class BlobIdTest {
  private static final Random random = new Random();
  private static final List<Short> versions = Arrays.asList(new Short[]{BLOB_ID_V1, BLOB_ID_V2});
  private static final List<Boolean> setterChoice = Arrays.asList(new Boolean[]{true, false});
  private byte referenceFlag;
  private byte referenceDatacenterId;
  private short referenceAccountId;
  private short referenceContainerId;
  private byte updatedFlag;
  private byte updatedDatacenterId;
  private short updatedAccountId;
  private short updatedContainerId;
  private PartitionId updatedPartitionId;
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
    updatedFlag = referenceFlag == Byte.MAX_VALUE ? 0 : (byte) (referenceFlag + 1);
    updatedDatacenterId = referenceDatacenterId == Byte.MAX_VALUE ? 0 : (byte) (referenceDatacenterId + 1);
    updatedAccountId = referenceAccountId == Short.MAX_VALUE ? 0 : (byte) (referenceAccountId + 1);
    updatedContainerId = referenceContainerId == Short.MAX_VALUE ? 0 : (byte) (referenceContainerId + 1);
    referencePartitionId = referenceClusterMap.getWritablePartitionIds().get(0);
    updatedPartitionId = referenceClusterMap.getWritablePartitionIds().get(1);
  }

  /**
   * Tests building blobId in both {@link BlobId#BLOB_ID_V1} and {@link BlobId#BLOB_ID_V2} using and without using
   * setters of {@link BlobIdBuilder}. The expected values for {@code flag}, {@code datacenterId}, {@code accountId},
   * {@code containerId}, and {@code partitionId} are listed below:
   * <pre>
   *                                          IfUseSetter
   * Version            true                                              false
   * 1            always default values except partitionId      always default values except partitionId
   * 2            values set by setters                         always default values except partitionId
   * </pre>
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testBuildBlobId() throws Exception {
    for (Short version : versions) {
      for (Boolean shouldUseSetter : setterChoice) {
        buildBlobIdAndAssert(version, referenceFlag, referenceDatacenterId, referenceAccountId, referenceContainerId,
            referencePartitionId, shouldUseSetter);
        buildBlobIdAndAssert(version, null, referenceDatacenterId, referenceAccountId, referenceContainerId,
            referencePartitionId, shouldUseSetter);
        buildBlobIdAndAssert(version, referenceFlag, null, referenceAccountId, referenceContainerId,
            referencePartitionId, shouldUseSetter);
        buildBlobIdAndAssert(version, referenceFlag, referenceDatacenterId, null, referenceContainerId,
            referencePartitionId, shouldUseSetter);
        buildBlobIdAndAssert(version, referenceFlag, referenceDatacenterId, referenceAccountId, null,
            referencePartitionId, shouldUseSetter);
        buildBlobIdAndAssert(version, null, null, null, null, referencePartitionId, shouldUseSetter);
      }
    }
  }

  /**
   * Tests when building blobId in {@link BlobId#BLOB_ID_V2}, the internal states of a {@link BlobIdBuilder} can
   * be re-set to other values after they were set to some values previously.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testSetAndBuildV2() throws Exception {
    BlobIdBuilder blobIdBuilderV2 = new BlobIdBuilderV2(referencePartitionId).setFlag(referenceFlag)
        .setDatacenterId(referenceDatacenterId)
        .setAccountId(referenceAccountId)
        .setContainerId(referenceContainerId);
    BlobId blobId = blobIdBuilderV2.build();
    assertBlob(BLOB_ID_V2, blobId, referenceFlag, referenceDatacenterId, referenceAccountId, referenceContainerId,
        referencePartitionId, true);
    // setter will set the same BlobIdBuilder internal states to different values
    blobId = blobIdBuilderV2.setFlag(updatedFlag)
        .setDatacenterId(updatedDatacenterId)
        .setAccountId(updatedAccountId)
        .setContainerId(updatedContainerId)
        .setPartitionId(updatedPartitionId)
        .build();
    assertBlob(BLOB_ID_V2, blobId, updatedFlag, updatedDatacenterId, updatedAccountId, updatedContainerId,
        updatedPartitionId, true);
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
        srcBlobIdStr = new BlobIdBuilderV2(referencePartitionId).setFlag(referenceFlag)
            .setDatacenterId(referenceDatacenterId)
            .setAccountId(referenceAccountId)
            .setContainerId(referenceContainerId)
            .build()
            .getID();
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
    // Empty blobId
    blobIdLikes.add("");
    // short Blob ID
    blobIdLikes.add("AA");

    for (String blobIdLike : blobIdLikes) {
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
   * @param useSetter {@code true} indicates using setter; {@code false} otherwise.
   * @throws Exception Any unexpected exception.
   */
  private void buildBlobIdAndAssert(short version, Byte flag, Byte datacenterId, Short accountId, Short containerId,
      PartitionId partitionId, boolean useSetter) throws Exception {
    BlobIdBuilder blobIdBuilder;
    switch (version) {
      case BLOB_ID_V1:
        blobIdBuilder = new BlobIdBuilder(partitionId);
        break;

      case BLOB_ID_V2:
        blobIdBuilder = new BlobIdBuilderV2(partitionId);
        break;

      default:
        throw new IllegalArgumentException("Invalid version number blob" + version);
    }
    if (useSetter) {
      blobIdBuilder.setFlag(flag).setDatacenterId(datacenterId).setAccountId(accountId).setContainerId(containerId);
    }
    BlobId blobId = blobIdBuilder.build();
    assertEquals("Wrong blobId version", version, getVersionFromBlobString(blobId.getID()));
    assertBlob(version, blobId, flag, datacenterId, accountId, containerId, partitionId, useSetter);
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
   * @param useSetter {@code true} indicates that setters of {@link BlobIdBuilder} were used the blobId was built,
   *                              {@code false} otherwise.
   * @throws Exception Any unexpected exception.
   */
  private void assertBlob(short version, BlobId blobId, Byte flag, Byte datacenterId, Short accountId,
      Short containerId, PartitionId partitionId, boolean useSetter) throws Exception {
    assertTrue("Used unrecognized version", version == BLOB_ID_V1 || version == BLOB_ID_V2);
    assertEquals("Wrong partition id in blobId: " + blobId, partitionId, blobId.getPartition());
    assertEquals("Wrong version in blobId.", version, blobId.getCurrentVersion());
    if (!useSetter || version == BLOB_ID_V1) {
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
        blobId = new BlobIdBuilder(referencePartitionId).build();
        break;

      case BLOB_ID_V2:
        blobId = new BlobIdBuilderV2(referencePartitionId).setFlag(referenceFlag)
            .setDatacenterId(referenceDatacenterId)
            .setAccountId(referenceAccountId)
            .setContainerId(referenceContainerId)
            .build();
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
    BlobIdV2(Byte flag, Byte datacenterId, Short accountId, Short containerId, PartitionId partitionId) {
      super(flag, datacenterId, accountId, containerId, partitionId);
    }

    @Override
    short getCurrentVersion() {
      return BLOB_ID_V2;
    }
  }

  /**
   * A BlobIdBuilder that builds a blobId as if its current version is set to {@link BlobId#BLOB_ID_V2}.
   */
  private class BlobIdBuilderV2 extends BlobIdBuilder {

    /**
     * Constructor
     * @param partitionId The partitionId to build a blobId.
     */
    BlobIdBuilderV2(PartitionId partitionId) {
      super(partitionId);
    }

    @Override
    public BlobId build() {
      return new BlobIdV2(flag, datacenterId, accountId, containerId, partitionId);
    }
  }
}
