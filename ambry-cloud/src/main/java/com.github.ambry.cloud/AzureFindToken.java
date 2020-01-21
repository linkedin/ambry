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
package com.github.ambry.cloud;

import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;


/**
 * Class representing the replication token to track replication progress in azure.
 */
public class AzureFindToken {
  private final String startContinuationToken;
  private final String endContinuationToken;
  private final int index;
  private final int totalItems;
  private final String azureTokenRequestId;
  private final short version;

  public static short VERSION_0 = 0;
  public static short DEFAULT_VERSION = VERSION_0;

  /**
   * Default constructor to create a {@link AzureFindToken} with uninitialized continuation token.
   */
  public AzureFindToken() {
    startContinuationToken = null;
    index = -1;
    endContinuationToken = null;
    totalItems = -1;
    azureTokenRequestId = null;
    version = DEFAULT_VERSION;
  }

  /**
   * Create {@link AzureFindToken} from provided values.
   * @param startContinuationToken
   * @param endContinuationToken
   * @param index
   * @param totalItems
   * @param azureTokenRequestId
   */
  public AzureFindToken(String startContinuationToken, String endContinuationToken, int index, int totalItems,
      String azureTokenRequestId) {
    this.startContinuationToken = startContinuationToken;
    this.endContinuationToken = endContinuationToken;
    this.index = index;
    this.totalItems = totalItems;
    this.azureTokenRequestId = azureTokenRequestId;
    this.version = DEFAULT_VERSION;
  }

  /**
   * Constructor to create a {@link AzureFindToken} with specified token values and specified version.
   * @param startContinuationToken
   * @param endContinuationToken
   * @param index
   * @param totalItems
   * @param azureTokenRequestId
   * @param version
   */
  public AzureFindToken(String startContinuationToken, String endContinuationToken, int index, int totalItems,
      String azureTokenRequestId, short version) {
    this.startContinuationToken = startContinuationToken;
    this.endContinuationToken = endContinuationToken;
    this.index = index;
    this.totalItems = totalItems;
    this.azureTokenRequestId = azureTokenRequestId;
    this.version = version;
  }

  /**
   * Deserialize {@link AzureFindToken} object from input stream.
   * @param inputStream {@link DataOutputStream} to deserialize from.
   * @return {@link AzureFindToken} object.
   * @throws IOException
   */
  public static AzureFindToken fromBytes(DataInputStream inputStream) throws IOException {
    short version = inputStream.readShort();
    String startContinuationToken = extractStringFromStream(inputStream);
    String endContinuationToken = extractStringFromStream(inputStream);
    int index = inputStream.readInt();
    int totalItems = inputStream.readInt();
    String azureTokenRequestId = extractStringFromStream(inputStream);
    return new AzureFindToken(startContinuationToken, endContinuationToken, index, totalItems, azureTokenRequestId,
        version);
  }

  /**
   * Extract string from the {@link DataInputStream}.
   * @param inputStream {@link DataInputStream} to extract String from.
   * @return extracted String from {@code inputStream}.
   * @throws IOException
   */
  private static String extractStringFromStream(DataInputStream inputStream) throws IOException {
    int size = inputStream.readInt();
    byte[] bytes = new byte[size];
    inputStream.read(bytes);
    return new String(bytes);
  }

  public byte[] toBytes() {
    byte[] buf = new byte[size()];
    ByteBuffer bufWrap = ByteBuffer.wrap(buf);
    bufWrap.putShort(version);
    bufWrap.putInt(startContinuationToken.length());
    bufWrap.put(startContinuationToken.getBytes());
    bufWrap.putInt(endContinuationToken.length());
    bufWrap.put(endContinuationToken.getBytes());
    bufWrap.putInt(index);
    bufWrap.putInt(totalItems);
    bufWrap.putInt(azureTokenRequestId.length());
    bufWrap.put(azureTokenRequestId.getBytes());
    return buf;
  }

  public short getVersion() {
    return version;
  }

  public int size() {
    return Short.BYTES + 5 * Integer.BYTES + startContinuationToken.length() + endContinuationToken.length()
        + azureTokenRequestId.length();
  }

  public boolean equals(AzureFindToken azureFindToken) {
    return azureFindToken.getVersion() == version && Utils.checkNullableStringEquals(
        azureFindToken.getStartContinuationToken(), startContinuationToken) && Utils.checkNullableStringEquals(
        azureFindToken.getEndContinuationToken(), endContinuationToken) && azureFindToken.getTotalItems() == totalItems
        && azureFindToken.getIndex() == index && Utils.checkNullableStringEquals(
        azureFindToken.getAzureTokenRequestId(), azureTokenRequestId);
  }

  /**
   * Return startContinuationToken of the current token.
   * @return startContinuationToken.
   */
  public String getStartContinuationToken() {
    return startContinuationToken;
  }

  /**
   * Return endContinuationToken of the current token.
   * @return endContinuationToken.
   */
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

  /**
   * Return azureTokenRequestId of the current token.
   * @return azureTokenRequestId.
   */
  public String getAzureTokenRequestId() {
    return azureTokenRequestId;
  }

  @Override
  public int hashCode() {
    return Objects.hash(startContinuationToken, endContinuationToken, index, totalItems, azureTokenRequestId, version);
  }
}
