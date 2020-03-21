/*
 * Copyright 2018 LinkedIn Corp. All rights reserved.
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

package com.github.ambry.frontend;

import com.github.ambry.utils.Pair;
import java.util.HashMap;
import java.util.Map;
import org.json.JSONException;
import org.json.JSONObject;


/**
 * Methods to serialize and deserialize JSON strings that contain a blob ID and metadata dictionary. These methods can
 * be used to create serialization formats in implementations of {@link IdSigningService}. Keep in mind that the strings
 * created by these methods are not URL-safe, signed, or encrypted, and that the {@link IdSigningService} implementation
 * should choose how to handle these concerns.
 */
public class SignedIdSerDe {
  private static String BLOB_ID_FIELD = "blobId";
  private static String METADATA_FIELD = "metadata";

  /**
   * @param blobId the blob ID to include in the JSON string.
   * @param metadata additional parameters to include in the JSON string.
   * @return a JSON string containing the blob ID and additional parameters.
   * @throws JSONException
   */
  public static String toJson(String blobId, Map<String, String> metadata) throws JSONException {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put(BLOB_ID_FIELD, blobId);
    if (!metadata.isEmpty()) {
      jsonObject.put(METADATA_FIELD, metadata);
    }
    return jsonObject.toString();
  }

  /**
   * @param jsonString the JSON string to deserialize.
   * @return a {@link Pair} that contains the blob ID and additional metadata parsed from the JSON string.
   * @throws JSONException
   */
  public static Pair<String, Map<String, String>> fromJson(String jsonString) throws JSONException {
    JSONObject jsonObject = new JSONObject(jsonString);
    String blobId = jsonObject.getString(BLOB_ID_FIELD);
    JSONObject metadataJson = jsonObject.optJSONObject(METADATA_FIELD);
    Map<String, String> metadata = new HashMap<>();
    if (metadataJson != null) {
      for (String key : metadataJson.keySet()) {
        metadata.put(key, metadataJson.getString(key));
      }
    }
    return new Pair<>(blobId, metadata);
  }
}
