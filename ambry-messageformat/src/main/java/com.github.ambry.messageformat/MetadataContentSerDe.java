/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.messageformat;

import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.Pair;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;


/**
 * A class to serialize and deserialize MetadataContent which forms the content of a Metadata Blob.
 */
public class MetadataContentSerDe {

  /**
   * Serialize the input list of keys that form the metadata content.
   * @param chunkSize the size of the intermediate data chunks for the object this metadata describes.
   * @param totalSize the total size of the object this metadata describes.
   * @param keys the input list of keys that form the metadata content.
   * @return a ByteBuffer containing the serialized output.
   */
  public static ByteBuffer serializeMetadataContentV2(int chunkSize, long totalSize, List<StoreKey> keys) {
    int bufSize =
        MessageFormatRecord.Metadata_Content_Format_V2.getMetadataContentSize(keys.get(0).sizeInBytes(), keys.size());
    ByteBuffer outputBuf = ByteBuffer.allocate(bufSize);
    MessageFormatRecord.Metadata_Content_Format_V2.serializeMetadataContentRecord(outputBuf, chunkSize, totalSize,
        keys);
    return outputBuf;
  }

  /**
   * Serialize the input list of keys with data content sizes that form the metadata content.
   * @param totalSize the total size of the object this metadata describes.
   * @param keysAndContentSizes the input list of keys and related data content sizes that form the metadata content.
   * @return a ByteBuffer containing the serialized output.
   */
  public static ByteBuffer serializeMetadataContentV3(long totalSize, List<Pair<StoreKey, Long>> keysAndContentSizes) {
    int bufSize = MessageFormatRecord.Metadata_Content_Format_V3.getMetadataContentSize(
        keysAndContentSizes.get(0).getFirst().sizeInBytes(), keysAndContentSizes.size());
    ByteBuffer outputBuf = ByteBuffer.allocate(bufSize);
    MessageFormatRecord.Metadata_Content_Format_V3.serializeMetadataContentRecord(outputBuf, totalSize,
        keysAndContentSizes);
    return outputBuf;
  }

  /**
   * Deserialize the serialized metadata content in the input ByteBuffer using the given {@link StoreKeyFactory} as a
   * reference.
   * @param buf ByteBuffer containing the serialized metadata content.
   * @param storeKeyFactory the {@link StoreKeyFactory} to use to deserialize the content.
   * @return a list of {@link StoreKey} containing the deserialized output.
   * @throws IOException if an IOException is encountered during deserialization.
   * @throws MessageFormatException if an unknown version is encountered in the header of the serialized input.
   */
  public static CompositeBlobInfo deserializeMetadataContentRecord(ByteBuffer buf, StoreKeyFactory storeKeyFactory)
      throws IOException, MessageFormatException {
    int version = buf.getShort();
    switch (version) {
      case MessageFormatRecord.Metadata_Content_Version_V2:
        return MessageFormatRecord.Metadata_Content_Format_V2.deserializeMetadataContentRecord(
            new DataInputStream(new ByteBufferInputStream(buf)), storeKeyFactory);
      case MessageFormatRecord.Metadata_Content_Version_V3:
        return MessageFormatRecord.Metadata_Content_Format_V3.deserializeMetadataContentRecord(
            new DataInputStream(new ByteBufferInputStream(buf)), storeKeyFactory);
      default:
        throw new MessageFormatException("Unknown version encountered for MetadataContent: " + version,
            MessageFormatErrorCodes.Unknown_Format_Version);
    }
  }
}
