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
 *
 */

package com.github.ambry.frontend;

import java.util.AbstractList;
import java.util.Collections;
import java.util.List;
import org.json.JSONArray;
import org.json.JSONObject;


/**
 * Methods to serialize and deserialize the body of a stitch request. The body of the request should be a JSON object
 * that contains an array field with the key, "signedChunkIds". Each element of the array should be a
 * signed ID produced by an {@link IdSigningService} implementation representing a data chunk to be stitched together.
 * The order of the IDs in the array will be the order in which the data chunks are stitched. For example:
 * <pre><code>
 * {
 *   "signedChunkIds": ["/signedId/id1", "/signedId/id2", "..."]
 * }
 * </code></pre>
 */
public class StitchRequestSerDe {
  private static final String SIGNED_CHUNK_IDS_KEY = "signedChunkIds";

  private StitchRequestSerDe() {
    // empty private constructor for a class that contains only static functions
  }

  /**
   * @param signedChunkIds the list of signed chunk IDs to stitch together.
   * @return a {@link JSONObject} representing the request.
   */
  public static JSONObject toJson(List<String> signedChunkIds) {
    return new JSONObject().put(SIGNED_CHUNK_IDS_KEY, signedChunkIds);
  }

  /**
   * @param stitchRequestJson the JSON object to extract the list of signed chunk IDs from.
   * @return a {@link List} that contains the signed chunk IDs to be stitched.
   */
  public static List<String> fromJson(JSONObject stitchRequestJson) {
    JSONArray signedChunkIds = stitchRequestJson.optJSONArray(StitchRequestSerDe.SIGNED_CHUNK_IDS_KEY);
    if (signedChunkIds == null) {
      return Collections.emptyList();
    } else {
      return new AbstractList<String>() {
        @Override
        public String get(int index) {
          return signedChunkIds.getString(index);
        }

        @Override
        public int size() {
          return signedChunkIds.length();
        }
      };
    }
  }
}
