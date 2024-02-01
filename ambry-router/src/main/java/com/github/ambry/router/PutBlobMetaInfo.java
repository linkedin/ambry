/*
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

package com.github.ambry.router;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ambry.utils.Pair;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.json.JSONArray;
import org.json.JSONObject;


/**
 * This class holds information about the data chunk list or other metadata of the PutOperation
 * It contains a list of key size pair for the blob's data chunks, reserved metadata etc.
 */
public class PutBlobMetaInfo {
  private static final String BLOB = "blob";
  private static final String SIZE = "size";
  private static final String CHUNKS = "chunks";
  private static final String RESERVED_METADATA_CHUNK_ID = "reservedMetadataChunkId";

  // reserved metadata chunk id
  private final String reservedMetadataChunkId;
  // the list of data chunk pairs <blob id, blob size> in order
  private final List<Pair<String, Long>> orderedChunkIdSizeList;
  // TODO [S3] Do we need ttl or sessionId?
  private static final ObjectMapper objectMapper = new ObjectMapper();

  /**
   * Construct a {@link PutBlobMetaInfo} object.
   * @param orderedChunkIdSizeList list of store keys and the size of the data content they reference
   * @param reservedMetadataChunkId the id of the reserved metadata chunk
   */
  public PutBlobMetaInfo(List<Pair<String, Long>> orderedChunkIdSizeList, String reservedMetadataChunkId) {
    this.orderedChunkIdSizeList = orderedChunkIdSizeList;
    this.reservedMetadataChunkId = reservedMetadataChunkId;
  }

  /**
   * Get the reserved metadata chunk id
   * @return the chunk id
   */
  public String getReservedMetadataChunkId() {
    return reservedMetadataChunkId;
  }

  /**
   * Get the number of chunks
   * @return the chunk number
   */
  public int getChunkNumber() {
    return orderedChunkIdSizeList.size();
  }

  /**
   * Get the chunk list
   * @return the list of chunk with blob id and blob size
   */
  public List<Pair<String, Long>> getOrderedChunkIdSizeList() {
    return orderedChunkIdSizeList;
  }

  /**
   * Serialize a {@link PutBlobMetaInfo} object to a JSON string
   * @param metaInfo the {@link PutBlobMetaInfo}
   * @return the JSON string representation of the object
   */
  public static String serialize(PutBlobMetaInfo metaInfo) {
    JSONObject rootObject = new JSONObject();

    // The order of elements in JSON arrays is preserved.
    // https://www.rfc-editor.org/rfc/rfc7159.html
    // An object is an unordered collection of zero or more name/value pairs
    // An array is an ordered sequence of zero or more values.
    JSONArray chunks = new JSONArray();
    for (Pair<String, Long> blobAndSize : metaInfo.orderedChunkIdSizeList) {
      chunks.put(new JSONObject().put(BLOB, blobAndSize.getFirst()).put(SIZE, blobAndSize.getSecond()));
    }
    rootObject.put(CHUNKS, chunks);
    rootObject.put(RESERVED_METADATA_CHUNK_ID, metaInfo.reservedMetadataChunkId);

    return rootObject.toString();
  }

  /**
   * Deserialize a JSON string to a {@link PutBlobMetaInfo} object.
   * @param metaInfo the JSON string representation of the object
   * @return the {@link PutBlobMetaInfo}
   */
  public static PutBlobMetaInfo deserialize(String metaInfo) throws IOException {
    List<Pair<String, Long>> orderedChunkIdSizeList = new ArrayList<>();

    JsonNode rootNode;
    try {
      rootNode = objectMapper.readTree(metaInfo);
    } catch (JsonProcessingException e) {
      throw new IOException("Not expected JSON content " + metaInfo + e.getMessage());
    }

    // loop all the data chunks
    JsonNode jsonNode = rootNode.get(CHUNKS);
    if (jsonNode.isArray()) {
      for (final JsonNode objNode : jsonNode) {
        String blobId = objNode.get(BLOB).textValue();
        Long chunkSizeBytes = objNode.get(SIZE).longValue();
        orderedChunkIdSizeList.add(new Pair<>(blobId, chunkSizeBytes));
      }
    } else {
      throw new IOException("Not expected JSON content " + metaInfo);
    }

    jsonNode = rootNode.get(RESERVED_METADATA_CHUNK_ID);
    String metaBlobId = jsonNode.textValue();
    return new PutBlobMetaInfo(orderedChunkIdSizeList, metaBlobId);
  }

  @Override
  public String toString() {
    return PutBlobMetaInfo.serialize(this);
  }
}
