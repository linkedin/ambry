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
import java.lang.reflect.Field;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.LinkedHashMap;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RecoveryToken implements FindToken {
  private final Logger logger = LoggerFactory.getLogger(RecoveryToken.class);
  // Maintain alphabetical order of fields for rapid debugging
  public static final String AMBRY_PARTITION_ID = "ambry_partition_id";
  public static final String AZURE_STORAGE_CONTAINER_ID = "azure_storage_container_id";
  public static final String AZURE_STORAGE_CONTINUATION_TOKEN = "azure_storage_continuation_token";
  public static final String END_OF_PARTITION = "end_of_partition";
  public static final String NUM_BLOBS = "num_blobs";
  public static final String NUM_BLOB_BYTES = "num_blob_bytes";
  public static final String RECOVERY_BEGIN_TIME = "recovery_begin_time";
  public static final String RECOVERY_END_TIME = "recovery_end_time";
  public static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy_MMM_dd_HH_mm_ss_SSS");

  private String azureToken = null;
  private String recoveryEndTime = null;
  private String recoveryStartTime = DATE_FORMAT.format(System.currentTimeMillis());
  private boolean endOfPartition = false;
  private long numBlobBytes = 0;
  private long numBlobs = 0;

  public RecoveryToken(RecoveryToken recoveryToken, String azureToken, long numBlobsDelta, long numBlobBytesDelta) {
    this.azureToken = azureToken;
    this.endOfPartition = azureToken != null && !azureToken.isEmpty();
    this.numBlobBytes = recoveryToken.numBlobBytes + numBlobBytesDelta;
    this.numBlobs = recoveryToken.numBlobs + numBlobsDelta;
    this.recoveryEndTime = recoveryToken.recoveryEndTime;
    this.recoveryStartTime = recoveryToken.recoveryStartTime;
  }

  public RecoveryToken(JSONObject jsonObject) {
    this.azureToken = jsonObject.getString(AZURE_STORAGE_CONTINUATION_TOKEN);
    this.endOfPartition = jsonObject.getBoolean(END_OF_PARTITION);
    this.numBlobBytes = jsonObject.getLong(NUM_BLOB_BYTES);
    this.numBlobs = jsonObject.getLong(NUM_BLOBS);
    this.recoveryEndTime = jsonObject.getString(RECOVERY_END_TIME);
    this.recoveryStartTime = jsonObject.getString(RECOVERY_BEGIN_TIME);
  }

  public RecoveryToken() {}

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
    // Maintain alphabetical order of fields for rapid debugging
    jsonObject.put(AZURE_STORAGE_CONTINUATION_TOKEN, azureToken);
    jsonObject.put(END_OF_PARTITION, endOfPartition);
    jsonObject.put(NUM_BLOBS, numBlobs);
    jsonObject.put(NUM_BLOB_BYTES, numBlobBytes);
    jsonObject.put(RECOVERY_BEGIN_TIME, recoveryStartTime);
    jsonObject.put(RECOVERY_END_TIME, recoveryEndTime);
    // Pretty print with indent for easy viewing
    return jsonObject.toString(4);
  }

  public String getToken() {
    return azureToken;
  }

  @Override
  public byte[] toBytes() {
    return new byte[0];
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
}
