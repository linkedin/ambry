/**
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.cloud.azure;

import com.github.ambry.replication.FindToken;
import com.github.ambry.replication.FindTokenType;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

import static com.github.ambry.utils.Utils.*;


/**
 * Class representing the replication token to track replication progress in azure.
 */
public class CosmosChangeFeedFindToken implements FindToken {
  private final short version;
  private final long bytesRead;
  private final FindTokenType findTokenType = FindTokenType.CloudBased;
  private final String startContinuationToken;
  private final String endContinuationToken;
  private final int index;
  private final int totalItems;
  private final String azureTokenRequestId;

  private final static short VERSION_0 = 0;
  private final static short DEFAULT_VERSION = VERSION_0;

  /**
   * Default constructor to create a {@link CosmosChangeFeedFindToken} with uninitialized continuation token.
   */
  public CosmosChangeFeedFindToken() {
    version = DEFAULT_VERSION;
    bytesRead = 0;
    startContinuationToken = "";
    index = -1;
    endContinuationToken = "";
    totalItems = -1;
    azureTokenRequestId = "";
  }

  /**
   * Create {@link CosmosChangeFeedFindToken} from provided values.
   * @param bytesRead bytes read by remote so far.
   * @param startContinuationToken start token from cosmos.
   * @param endContinuationToken end token from cosmos.
   * @param index index in cache upto which items are consumed by remote.
   * @param totalItems total number of items in cache.
   * @param azureTokenRequestId request id of the change feed query.
   */
  public CosmosChangeFeedFindToken(long bytesRead, String startContinuationToken, String endContinuationToken,
      int index, int totalItems, String azureTokenRequestId) {
    this.version = DEFAULT_VERSION;
    this.bytesRead = bytesRead;
    this.startContinuationToken = startContinuationToken;
    this.endContinuationToken = endContinuationToken;
    this.index = index;
    this.totalItems = totalItems;
    this.azureTokenRequestId = azureTokenRequestId;
  }

  /**
   * Constructor to create a {@link CosmosChangeFeedFindToken} with specified token values and specified version.
   * @param bytesRead bytes read by remote so far.
   * @param startContinuationToken start token from cosmos.
   * @param endContinuationToken end token from cosmos.
   * @param index index in cache upto which items are consumed by remote.
   * @param totalItems total number of items in cache.
   * @param azureTokenRequestId request id of the change feed query.
   * @param version token version.
   */
  public CosmosChangeFeedFindToken(long bytesRead, String startContinuationToken, String endContinuationToken,
      int index, int totalItems, String azureTokenRequestId, short version) {
    this.version = version;
    this.bytesRead = bytesRead;
    this.startContinuationToken = startContinuationToken;
    this.endContinuationToken = endContinuationToken;
    this.index = index;
    this.totalItems = totalItems;
    this.azureTokenRequestId = azureTokenRequestId;
  }

  /**
   * Deserialize {@link CosmosChangeFeedFindToken} object from input stream.
   * @param inputStream {@link DataOutputStream} to deserialize from.
   * @return {@link CosmosChangeFeedFindToken} object.
   * @throws IOException
   */
  public static CosmosChangeFeedFindToken fromBytes(DataInputStream inputStream) throws IOException {
    CosmosChangeFeedFindToken cosmosChangeFeedFindToken;
    DataInputStream stream = new DataInputStream(inputStream);
    short version = stream.readShort();
    switch (version) {
      case VERSION_0:
        FindTokenType type = FindTokenType.values()[stream.readShort()];
        if (type != FindTokenType.CloudBased) {
          throw new IllegalArgumentException(
              String.format("Invalid token type %s found while deserialization. Expected %s.", type,
                  FindTokenType.CloudBased));
        }
        long bytesRead = stream.readLong();
        String startContinuationToken = extractStringFromStream(inputStream);
        String endContinuationToken = extractStringFromStream(inputStream);
        int index = inputStream.readInt();
        int totalItems = inputStream.readInt();
        String azureTokenRequestId = extractStringFromStream(inputStream);
        cosmosChangeFeedFindToken =
            new CosmosChangeFeedFindToken(bytesRead, startContinuationToken, endContinuationToken, index, totalItems,
                azureTokenRequestId, version);
        break;
      default:
        throw new IllegalStateException("Unknown version of cloud token: " + version);
    }
    return cosmosChangeFeedFindToken;
  }

  /**
   * Extract string from the {@link DataInputStream}.
   * @param inputStream {@link DataInputStream} to extract String from.
   * @return extracted String from {@code inputStream}.
   * @throws IOException in case of exception while reading from stream.
   */
  private static String extractStringFromStream(DataInputStream inputStream) throws IOException {
    int size = inputStream.readInt();
    byte[] bytes = new byte[size];
    inputStream.read(bytes);
    return new String(bytes);
  }

  /**
   * Serialize {@link CosmosChangeFeedFindToken} to byte array.
   * @return serialized byte array.
   */
  public byte[] toBytes() {
    byte[] buf = new byte[size()];
    ByteBuffer bufWrap = ByteBuffer.wrap(buf);
    bufWrap.putShort(getVersion());
    bufWrap.putShort((short) getType().ordinal());
    bufWrap.putLong(getBytesRead());
    bufWrap.putInt(getNullableStringLength(startContinuationToken));
    bufWrap.put(nullableStringToBytes(startContinuationToken));
    bufWrap.putInt(getNullableStringLength(endContinuationToken));
    bufWrap.put(nullableStringToBytes(endContinuationToken));
    bufWrap.putInt(index);
    bufWrap.putInt(totalItems);
    bufWrap.putInt(getNullableStringLength(azureTokenRequestId));
    bufWrap.put(nullableStringToBytes(azureTokenRequestId));
    return buf;
  }

  /**
   * Calculate size of the token.
   * @return size of the token.
   */
  public int size() {
    return 2 * Short.BYTES + Long.BYTES + 5 * Integer.BYTES + getNullableStringLength(startContinuationToken)
        + getNullableStringLength(endContinuationToken) + getNullableStringLength(azureTokenRequestId);
  }

  /**
   * Utility to construct a new AzureFindToken from a previous instance and new token returned from findEntriesSince query.
   * @param newBytesRead bytes read in the findEntriesSince query.
   * @return the updated token.
   */
  public CosmosChangeFeedFindToken getUpdatedToken(long newBytesRead) {
    if (newBytesRead == 0) {
      return this;
    }
    return new CosmosChangeFeedFindToken();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CosmosChangeFeedFindToken cosmosChangeFeedFindToken = (CosmosChangeFeedFindToken) o;
    return cosmosChangeFeedFindToken.getVersion() == getVersion()
        && cosmosChangeFeedFindToken.getBytesRead() == getBytesRead() && checkNullableStringEquals(
        cosmosChangeFeedFindToken.getStartContinuationToken(), startContinuationToken) && checkNullableStringEquals(
        cosmosChangeFeedFindToken.getEndContinuationToken(), endContinuationToken)
        && cosmosChangeFeedFindToken.getTotalItems() == totalItems && cosmosChangeFeedFindToken.getIndex() == index
        && checkNullableStringEquals(cosmosChangeFeedFindToken.getAzureTokenRequestId(), azureTokenRequestId);
  }

  /**
   * Return startContinuationToken of the current token.
   * @return startContinuationToken.
   */
  public String getStartContinuationToken() {
    return startContinuationToken;
  }

  public String getEndContinuationToken() {
    return endContinuationToken;
  }

  /**
   * Return index of the current token.
   * @return index.
   */
  public int getIndex() {
    return index;
  }

  /**
   * Return totalitems in the current token.
   * @return totalitems.
   */
  public int getTotalItems() {
    return totalItems;
  }

  @Override
  public long getBytesRead() {
    return bytesRead;
  }

  @Override
  public FindTokenType getType() {
    return findTokenType;
  }

  @Override
  public short getVersion() {
    return version;
  }

  public FindTokenType getFindTokenType() {
    return findTokenType;
  }

  /**
   * Return azureTokenRequestId of the current token.
   * @return azureTokenRequestId.
   */
  public String getAzureTokenRequestId() {
    return azureTokenRequestId;
  }

  @Override
  public int hashCode() {
    return Objects.hash(startContinuationToken, endContinuationToken, index, totalItems, azureTokenRequestId,
        getVersion(), getBytesRead(), getType());
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("version: ").append(getVersion());
    sb.append(" bytesRead: ").append(getBytesRead());
    sb.append(" type: ").append(getType().toString());
    sb.append(" startContinuationToken: ").append(startContinuationToken);
    sb.append(" endContinuationToken: ").append(endContinuationToken);
    sb.append(" index: ").append(index);
    sb.append(" totalItems: ").append(totalItems);
    sb.append(" azureTokenRequestId: ").append(azureTokenRequestId);
    return sb.toString();
  }
}
