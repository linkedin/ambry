/**
 * Copyright 2024 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.replication.FindToken;
import com.github.ambry.replication.FindTokenType;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.LinkedHashMap;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RecoveryToken implements FindToken {
  private static final Logger logger = LoggerFactory.getLogger(RecoveryToken.class);
  // Maintain alphabetical order for convenience
  public static final String AMBRY_PARTITION_ID = "ambry_partition_id";
  public static final String AZURE_STORAGE_CONTAINER_ID = "azure_storage_container_id";
  public static final String AZURE_STORAGE_CONTINUATION_TOKEN = "azure_storage_continuation_token";
  public static final String END_OF_PARTITION = "end_of_partition";
  public static final String NUM_BLOBS = "num_blobs";
  public static final String NUM_BLOB_BYTES = "num_blob_bytes";
  public static final String RECOVERY_BEGIN_TIME = "recovery_begin_time";
  public static final String RECOVERY_END_TIME = "recovery_end_time";
  public static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy_MMM_dd_HH_mm_ss_SSS");

  // JSON.put ignores null strings, hence insert a string and change it back to null
  public static final String NONE = "none";
  private final long ambryPartitionId;
  private final String azureContainerId;
  private final String azureToken;
  private final String recoveryBeginTime;
  private final String recoveryEndTime;
  private final boolean endOfPartition;
  private final long numBlobBytes;
  private final long numBlobs;

  public RecoveryToken(RecoveryToken recoveryToken, long ambryPartitionId, String azureContainerId, String azureToken,
      long numBlobsDelta, long numBlobBytesDelta) {
    this.ambryPartitionId = ambryPartitionId;
    this.azureContainerId = azureContainerId;
    this.azureToken = azureToken;
    this.endOfPartition = azureToken == null;
    this.numBlobBytes = recoveryToken.numBlobBytes + numBlobBytesDelta;
    this.numBlobs = recoveryToken.numBlobs + numBlobsDelta;
    this.recoveryBeginTime = recoveryToken.recoveryBeginTime;
    this.recoveryEndTime = recoveryToken.recoveryEndTime;
  }

  public RecoveryToken(JSONObject jsonObject) {
    this.ambryPartitionId = jsonObject.getLong(AMBRY_PARTITION_ID);
    String azureContainerId = jsonObject.getString(AZURE_STORAGE_CONTAINER_ID);
    this.azureContainerId = azureContainerId.equals(NONE) ? null : azureContainerId;
    String azureToken = jsonObject.getString(AZURE_STORAGE_CONTINUATION_TOKEN);
    this.azureToken = azureToken.equals(NONE) ? null : azureToken;
    this.endOfPartition = jsonObject.getBoolean(END_OF_PARTITION);
    this.numBlobBytes = jsonObject.getLong(NUM_BLOB_BYTES);
    this.numBlobs = jsonObject.getLong(NUM_BLOBS);
    this.recoveryBeginTime = jsonObject.getString(RECOVERY_BEGIN_TIME);
    String recoveryEndTime = jsonObject.getString(RECOVERY_END_TIME);
    this.recoveryEndTime = recoveryEndTime.equals(NONE) ? null : recoveryEndTime;
  }

  public RecoveryToken() {
    this.ambryPartitionId = -1;
    this.azureContainerId = null;
    this.azureToken = null;
    this.endOfPartition = false;
    this.numBlobBytes = 0;
    this.numBlobs = 0;
    this.recoveryBeginTime = DATE_FORMAT.format(System.currentTimeMillis());
    this.recoveryEndTime = null;
  }

  public String toString() {
    JSONObject jsonObject = new JSONObject();
    try {
      // Ensures json fields are printed exactly in the order they are inserted
      Field changeMap = jsonObject.getClass().getDeclaredField("map");
      changeMap.setAccessible(true);
      changeMap.set(jsonObject, new LinkedHashMap<>());
      changeMap.setAccessible(false);
    } catch (IllegalAccessException | NoSuchFieldException e) {
      logger.error(e.getMessage());
      jsonObject = new JSONObject();
    }
    // Maintain alphabetical order of fields for string match
    jsonObject.put(AMBRY_PARTITION_ID, ambryPartitionId);
    jsonObject.put(AZURE_STORAGE_CONTAINER_ID, azureContainerId == null ? NONE : azureContainerId);
    jsonObject.put(AZURE_STORAGE_CONTINUATION_TOKEN, azureToken == null ? NONE : azureToken);
    jsonObject.put(END_OF_PARTITION, endOfPartition);
    jsonObject.put(NUM_BLOBS, numBlobs);
    jsonObject.put(NUM_BLOB_BYTES, numBlobBytes);
    jsonObject.put(RECOVERY_BEGIN_TIME, recoveryBeginTime);
    jsonObject.put(RECOVERY_END_TIME, recoveryEndTime == null ? NONE : recoveryEndTime);
    // Pretty print with indent for easy viewing
    return jsonObject.toString(4);
  }

  public long getAmbryPartitionId() {
    return ambryPartitionId;
  }

  public String getAzureStorageContainerId() {
    return azureContainerId;
  }

  public String getToken() {
    return azureToken;
  }

  public boolean isEndOfPartition() {
    return endOfPartition;
  }

  public static RecoveryToken fromBytes(DataInputStream stream) throws IOException {
    byte[] buf = new byte[stream.readInt()];
    stream.readFully(buf);
    String tokenString = new String(buf, Charset.defaultCharset());
    logger.trace(tokenString);
    JSONObject jsonObject = new JSONObject(tokenString);
    return new RecoveryToken(jsonObject);
  }

  @Override
  public byte[] toBytes() {
    String json = toString();
    byte[] buf = new byte[Integer.BYTES + json.length()];
    ByteBuffer bufWrap = ByteBuffer.wrap(buf);
    bufWrap.putInt(json.length());
    bufWrap.put(json.getBytes(Charset.defaultCharset()));
    return buf;
  }

  @Override
  public long getBytesRead() {
    return numBlobBytes;
  }

  @Override
  public FindTokenType getType() {
    return FindTokenType.CloudBased;
  }

  @Override
  public short getVersion() {
    return 0;
  }

  @Override
  public boolean equals(Object obj) {
    return this.toString().equals(obj.toString());
  }
}
