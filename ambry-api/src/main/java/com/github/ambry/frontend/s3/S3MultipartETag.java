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
 *
 */
package com.github.ambry.frontend.s3;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.ambry.frontend.PutBlobMetaInfo;
import com.github.ambry.utils.Pair;
import java.io.IOException;
import java.util.Base64;
import java.util.List;
import java.util.Objects;


/**
 * This class holds ETag returned for the Multipart part upload.
 * json example:
 *  {
 *    "dataChunkList": {
 *      "chunks": [
 *        {
 *          "blob": "AAYQ_3Z6AAoAAQAAAAAAAAAHyAWDw-xvSHWPD608xUQH3w",
 *          "size": 8024
 *        },
 *        {
 *          "blob": "AAYQ_3Z6ACWBADERGGGGGVVDASDEG-xv8xUQH3wAHyAWDw",
 *          "size": 4031
 *        }
 *      ],
 *      "reservedMetadataChunkId": null
 *    },
 *    "version": 1
 *  }
 */
public class S3MultipartETag {
  static final short VERSION_1 = 1;
  static short CURRENT_VERSION = VERSION_1;
  private static final String VERSION = "version";
  private static final String DATA_CHUNK_LIST = "dataChunkList";
  private final List<Pair<String, Long>> orderedChunkIdSizeList;
  private short version;

  private static final ObjectMapper objectMapper = new ObjectMapper();
  private static final Base64.Encoder BASE64_ENCODER_WITHOUT_PADDING = Base64.getUrlEncoder().withoutPadding();

  /**
   * Construct S3MultipartETag
   * @param orderedChunkIdSizeList data chunk id and size list in order
   */
  public S3MultipartETag(List<Pair<String, Long>> orderedChunkIdSizeList) {
    version = CURRENT_VERSION;
    this.orderedChunkIdSizeList = orderedChunkIdSizeList;
  }

  /**
   * Get the version number
   * @return the version
   */
  public short getVersion() {
    return version;
  }

  /**
   * Get the data chunk list in order
   * @return the data chunk id/size list
   */
  public List<Pair<String, Long>> getOrderedChunkIdSizeList() {
    return orderedChunkIdSizeList;
  }

  /**
   * Serialize the {@link S3MultipartETag} to Json string
   * @return the serialized Json string
   */
  public static String serialize(S3MultipartETag eTag) throws IOException {
    ObjectNode rootObject = objectMapper.createObjectNode();

    PutBlobMetaInfo metaInfo = new PutBlobMetaInfo(eTag.getOrderedChunkIdSizeList(), null);
    ObjectNode chunks = PutBlobMetaInfo.serializeToJsonObject(metaInfo);
    rootObject.put(DATA_CHUNK_LIST, chunks);
    rootObject.put(VERSION, CURRENT_VERSION);

    return BASE64_ENCODER_WITHOUT_PADDING.encodeToString(rootObject.toString().getBytes());
  }

  /**
   * Deserialize the Json String to {@link S3MultipartETag}
   * @return the {@link S3MultipartETag}
   */
  public static S3MultipartETag deserialize(String encodedETagStr) throws IOException {
    String eTagStr = new String(Base64.getUrlDecoder().decode(encodedETagStr));
    JsonNode rootNode;
    try {
      rootNode = objectMapper.readTree(eTagStr);
    } catch (JsonProcessingException e) {
      throw new IOException("Not expected JSON content " + eTagStr + e.getMessage());
    }

    // version id
    JsonNode jsonNode = rootNode.get(VERSION);
    short version = jsonNode.shortValue();
    if (version != VERSION_1) {
      throw new IOException("Wrong version number " + eTagStr);
    }

    // data chunk list
    jsonNode = rootNode.get(DATA_CHUNK_LIST);
    PutBlobMetaInfo putBlobMetaInfo = PutBlobMetaInfo.deserialize(jsonNode.toString());
    return new S3MultipartETag(putBlobMetaInfo.getOrderedChunkIdSizeList());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    S3MultipartETag other = (S3MultipartETag) o;

    return version == other.version && Objects.equals(orderedChunkIdSizeList, other.orderedChunkIdSizeList);
  }
}
