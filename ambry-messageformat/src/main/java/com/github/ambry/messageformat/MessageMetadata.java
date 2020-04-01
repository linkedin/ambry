/**
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;


/**
 * MessageFormat metadata associated with messages sent out.
 */
public class MessageMetadata {
  private static final short MESSAGE_METADATA_VERSION_V1 = 1;
  private final ByteBuffer encryptionKey;
  private final short version;

  MessageMetadata(short version, ByteBuffer encryptionKey) {
    this.version = version;
    this.encryptionKey = encryptionKey;
  }

  public MessageMetadata(ByteBuffer encryptionKey) {
    this(MESSAGE_METADATA_VERSION_V1, encryptionKey);
  }

  public ByteBuffer getEncryptionKey() {
    return encryptionKey;
  }

  public short getVersion() {
    return version;
  }

  /**
   * @return The number of bytes in the serialized form of this instance of MessageMetadata.
   */
  public int sizeInBytes() {
    return Short.BYTES + Integer.BYTES + (encryptionKey == null ? 0 : encryptionKey.remaining());
  }

  /**
   * Serialize the MessageMetadata into the given {@link ByteBuffer}
   * @param outputBuffer the {@link ByteBuffer} to which to write the serialized bytes into.
   */
  public void serializeMessageMetadata(ByteBuffer outputBuffer) {
    switch (version) {
      case MESSAGE_METADATA_VERSION_V1:
        outputBuffer.putShort(version);
        if (encryptionKey != null) {
          outputBuffer.putInt(encryptionKey.remaining()).put(encryptionKey);
        } else {
          outputBuffer.putInt(0);
        }
        break;
      default:
        throw new IllegalStateException("Unknown MessageMetadata version");
    }
  }

  /**
   * Deserialize MessageMetadata from the given {@link DataInputStream}
   * @param stream the stream to read bytes from.
   * @return the deserialized MessageMetadata.
   * @throws IOException if an error occurs reading from the stream.
   */
  public static MessageMetadata deserializeMessageMetadata(DataInputStream stream) throws IOException {
    MessageMetadata messageMetadata;
    short version = stream.readShort();
    switch (version) {
      case MESSAGE_METADATA_VERSION_V1:
        ByteBuffer encryptionKey = Utils.readIntBuffer(stream);
        // An empty encryption key could mean that the original before serialization was null or empty. Either way
        // denotes the absence of a key.
        messageMetadata = new MessageMetadata(version, encryptionKey);
        break;
      default:
        throw new IllegalStateException("Unknown MessageMetadata version");
    }
    return messageMetadata;
  }
}
