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

import com.github.ambry.cloud.CloudDestinationToken;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;


/**
 * Class representing the replication token to track replication progress in azure.
 */
public class AzureCloudDestinationToken implements CloudDestinationToken {
  private final String cosmosRequestContinuationToken;
  private final short version;

  public static short VERSION_0 = 0;
  public static short DEFAULT_VERSION = VERSION_0;

  /**
   * Default constructor to create a {@link AzureCloudDestinationToken} with uninitialized continuation token.
   */
  public AzureCloudDestinationToken() {
    cosmosRequestContinuationToken = null;
    version = VERSION_0;
  }

  /**
   * Constructor to create a {@link AzureCloudDestinationToken} with specified continuation token.
   * @param cosmosRequestContinuationToken continuation token.
   */
  public AzureCloudDestinationToken(String cosmosRequestContinuationToken) {
    this.cosmosRequestContinuationToken = cosmosRequestContinuationToken;
    this.version = DEFAULT_VERSION;
  }

  /**
   * Constructor to create a {@link AzureCloudDestinationToken} with specified continuation token and specified version.
   * @param cosmosRequestContinuationToken continuation token.
   * @param version token version.
   */
  public AzureCloudDestinationToken(String cosmosRequestContinuationToken, short version) {
    this.cosmosRequestContinuationToken = cosmosRequestContinuationToken;
    this.version = version;
  }

  /**
   * Deserialize {@link AzureCloudDestinationToken} object from input stream.
   * @param inputStream {@link DataOutputStream} to deserialize from.
   * @return {@link AzureCloudDestinationToken} object.
   * @throws IOException
   */
  static AzureCloudDestinationToken fromBytes(DataInputStream inputStream) throws IOException {
    short version = inputStream.readShort();
    int size = inputStream.readInt();
    byte[] bytes = new byte[size];
    inputStream.read(bytes);
    String cosmosRequestContinuationToken = Arrays.toString(bytes);
    return new AzureCloudDestinationToken(cosmosRequestContinuationToken, version);
  }

  @Override
  public byte[] toBytes() {
    byte[] buf = new byte[size()];
    ByteBuffer bufWrap = ByteBuffer.wrap(buf);
    bufWrap.putShort(version);
    bufWrap.putInt(cosmosRequestContinuationToken.length());
    bufWrap.put(cosmosRequestContinuationToken.getBytes());
    return buf;
  }

  @Override
  public short getVersion() {
    return version;
  }

  @Override
  public int size() {
    return Short.BYTES + Integer.BYTES + cosmosRequestContinuationToken.length();
  }

  @Override
  public boolean equals(CloudDestinationToken otherCloudDestinationToken) {
    if(!(otherCloudDestinationToken instanceof AzureCloudDestinationToken)) {
      throw new IllegalArgumentException("cannot compare if argument is not AzureCloudDestinationToken");
    }
    AzureCloudDestinationToken azureCloudDestinationToken = (AzureCloudDestinationToken) otherCloudDestinationToken;
    return azureCloudDestinationToken.getVersion() == version
        && azureCloudDestinationToken.getCosmosRequestContinuationToken().equals(cosmosRequestContinuationToken);
  }

  /**
   * Return request continuation token.
   * @return request continuation token.
   */
  public String getCosmosRequestContinuationToken() {
    return cosmosRequestContinuationToken;
  }
}
