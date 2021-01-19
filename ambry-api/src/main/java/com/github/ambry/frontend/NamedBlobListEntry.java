/*
 * Copyright 2021 LinkedIn Corp. All rights reserved.
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
 *
 */

package com.github.ambry.frontend;

import com.github.ambry.named.NamedBlobRecord;
import com.github.ambry.utils.Utils;
import java.util.Objects;
import org.json.JSONObject;


/**
 * Represents an entry in a list of named blobs returned as an API response.
 */
public class NamedBlobListEntry {
  private static final String BLOB_NAME_KEY = "blobName";
  private static final String EXPIRATION_TIME_MS_KEY = "expirationTimeMs";

  private final String blobName;
  private final long expirationTimeMs;

  /**
   * Read a {@link NamedBlobRecord} from JSON.
   * @param jsonObject the {@link JSONObject} to deserialize.
   */
  public NamedBlobListEntry(JSONObject jsonObject) {
    this(jsonObject.getString(BLOB_NAME_KEY),
        jsonObject.optLong(EXPIRATION_TIME_MS_KEY, Utils.Infinite_Time));
  }

  /**
   * Convert a {@link NamedBlobRecord} into a {@link NamedBlobListEntry}.
   * @param record the {@link NamedBlobRecord}.
   */
  NamedBlobListEntry(NamedBlobRecord record) {
    this(record.getBlobName(), record.getExpirationTimeMs());
  }
  /**
   * @param blobName the blob name within a container.
   * @param expirationTimeMs the expiration time in milliseconds since epoch, or -1 if the blob should be permanent.
   */
  private NamedBlobListEntry(String blobName, long expirationTimeMs) {
    this.blobName = blobName;
    this.expirationTimeMs = expirationTimeMs;
  }

  /**
   * @return the blob name within a container.
   */
  public String getBlobName() {
    return blobName;
  }

  /**
   * @return the expiration time in milliseconds since epoch, or -1 if the blob should be permanent.
   */
  public long getExpirationTimeMs() {
    return expirationTimeMs;
  }

  /**
   * @return this list entry as a {@link JSONObject}.
   */
  public JSONObject toJson() {
    JSONObject jsonObject = new JSONObject().put(BLOB_NAME_KEY, blobName);
    if (expirationTimeMs != Utils.Infinite_Time) {
      jsonObject.put(EXPIRATION_TIME_MS_KEY, expirationTimeMs);
    }
    return jsonObject;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    NamedBlobListEntry that = (NamedBlobListEntry) o;
    return expirationTimeMs == that.expirationTimeMs && Objects.equals(blobName, that.blobName);
  }
}
