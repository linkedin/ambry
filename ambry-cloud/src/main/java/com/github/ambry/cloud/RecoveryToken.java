/**
 * Copyright 2023 LinkedIn Corp. All rights reserved.
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
  public static final String QUERY_NAME = "last_query_name";
  public static final String COSMOS_CONTINUATION_TOKEN = "cosmos_continuation_token";
  public static final String REQUEST_UNITS = "request_units_charged_so_far";
  public static final String NUM_ITEMS = "num_items_read_so_far";
  public static final String NUM_BLOB_BYTES = "blob_bytes_read_so_far";
  public static final String END_OF_PARTITION = "end_of_partition_reached";
  public static final String TOKEN_CREATE_TIME = "token_create_time_gmt";
  public static final String EARLIEST_BLOB = "earliest_blob_id";
  public static final String BACKUP_START_TIME = "earliest_blob_create_time_ms";
  public static final String BACKUP_START_TIME_GMT = "earliest_blob_create_time_gmt";

  public static final String LATEST_BLOB = "latest_blob_id";
  public static final String BACKUP_END_TIME = "latest_blob_update_time_ms";
  public static final String BACKUP_END_TIME_GMT = "latest_blob_update_time_gmt";

  public static final String LAST_QUERY_TIME = "last_query_time_gmt";
  public static final DateFormat DATE_FORMAT = new SimpleDateFormat("dd MMM yyyy HH:mm:ss:SSS");

  private String queryName = null;
  private String cosmosContinuationToken = null;
  private double requestUnits = 0;
  private long numItems = 0;
  private long numBlobBytes = 0;
  private boolean endOfPartitionReached = false;
  private final String tokenCreateTime;
  private long backupStartTime = -1;
  private long backupEndTime = -1;
  private String lastQueryTime = null;
  private String firstBlobId = null;
  private String lastBlobId = null;

  public RecoveryToken(String queryName, String cosmosContinuationToken, double requestUnits, long numItems,
      long numBytes, boolean endOfPartitionReached, String tokenCreateTime, long backupStartTime, long backupEndTime,
      long lastQueryTime, String firstBlobId, String lastBlobId) {
    this.queryName = queryName;
    this.cosmosContinuationToken = cosmosContinuationToken;
    this.requestUnits = requestUnits;
    this.numItems = numItems;
    this.numBlobBytes = numBytes;
    this.endOfPartitionReached = endOfPartitionReached;
    this.tokenCreateTime = tokenCreateTime;
    this.backupStartTime = backupStartTime;
    this.backupEndTime = backupEndTime;
    this.lastQueryTime = DATE_FORMAT.format(lastQueryTime);
    this.firstBlobId = firstBlobId;
    this.lastBlobId = lastBlobId;
  }

  public RecoveryToken(JSONObject jsonObject) {
    this.queryName = jsonObject.getString(QUERY_NAME);
    this.cosmosContinuationToken = jsonObject.getString(COSMOS_CONTINUATION_TOKEN);
    this.requestUnits = jsonObject.getDouble(REQUEST_UNITS);
    this.numItems = jsonObject.getLong(NUM_ITEMS);
    this.numBlobBytes = jsonObject.getLong(NUM_BLOB_BYTES);
    this.endOfPartitionReached = jsonObject.getBoolean(END_OF_PARTITION);
    this.tokenCreateTime = jsonObject.getString(TOKEN_CREATE_TIME);
    this.firstBlobId = jsonObject.getString(EARLIEST_BLOB);
    this.backupStartTime = jsonObject.getLong(BACKUP_START_TIME);
    this.lastBlobId = jsonObject.getString(LATEST_BLOB);
    this.backupEndTime = jsonObject.getLong(BACKUP_END_TIME);
    this.lastQueryTime = jsonObject.getString(LAST_QUERY_TIME);
  }

  public RecoveryToken() {
    this.tokenCreateTime = DATE_FORMAT.format(System.currentTimeMillis());
  }

  public String getQueryName() {
    return queryName;
  }

  public String getCosmosContinuationToken() {
    return cosmosContinuationToken;
  }

  public double getRequestUnits() {
    return requestUnits;
  }

  public long getNumItems() {
    return numItems;
  }

  public long getNumBlobBytes() {
    return numBlobBytes;
  }

  public boolean isEndOfPartitionReached() {
    return endOfPartitionReached;
  }

  public boolean isEmpty() {
    return getCosmosContinuationToken().isEmpty();
  }

  public String getTokenCreateTime() {
    return tokenCreateTime;
  }

  public long getBackupStartTimeMs() {
    return backupStartTime;
  }

  public long getBackupEndTimeMs() {
    return backupEndTime;
  }

  public String getEarliestBlob() {
    return firstBlobId;
  }

  public String getLatestBlob() {
    return lastBlobId;
  }

  public String toString() {
    JSONObject jsonObject = new JSONObject();
    try {
      Field changeMap = jsonObject.getClass().getDeclaredField("map");
      changeMap.setAccessible(true);
      changeMap.set(jsonObject, new LinkedHashMap<>());
      changeMap.setAccessible(false);
    } catch (IllegalAccessException | NoSuchFieldException e) {
      logger.error(e.getMessage());
      jsonObject = new JSONObject();
    }
    jsonObject.put(COSMOS_CONTINUATION_TOKEN, this.getCosmosContinuationToken());
    jsonObject.put(EARLIEST_BLOB, this.firstBlobId);
    jsonObject.put(BACKUP_START_TIME, backupStartTime);
    jsonObject.put(BACKUP_START_TIME_GMT, DATE_FORMAT.format(backupStartTime));
    jsonObject.put(LATEST_BLOB, this.lastBlobId);
    jsonObject.put(BACKUP_END_TIME, backupEndTime);
    jsonObject.put(BACKUP_END_TIME_GMT, DATE_FORMAT.format(backupEndTime));
    jsonObject.put(END_OF_PARTITION, this.isEndOfPartitionReached());
    jsonObject.put(QUERY_NAME, this.getQueryName());
    jsonObject.put(TOKEN_CREATE_TIME, this.getTokenCreateTime());
    jsonObject.put(LAST_QUERY_TIME, lastQueryTime);
    jsonObject.put(NUM_ITEMS, this.getNumItems());
    jsonObject.put(NUM_BLOB_BYTES, this.getNumBlobBytes());
    jsonObject.put(REQUEST_UNITS, this.getRequestUnits());
    return jsonObject.toString(4);
  }

  @Override
  public byte[] toBytes() {
    return new byte[0];
  }

  @Override
  public long getBytesRead() {
    return 0;
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
