/**
 * Copyright 2022 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.protocol;

import com.github.ambry.server.ServerErrorCode;
import java.io.DataInputStream;
import java.io.IOException;


/**
 * Representation of a response to an administration request, with content to return to client.
 * Currently only used to reply to {@link HealthCheckAdminRequest}.
 */
public class AdminResponseWithContent extends AdminResponse {
  private static final int CONTENT_LENGTH_SIZE = 4;
  private final byte[] content;

  /**
   * Constructs an {@link AdminResponseWithContent} with empty content.
   * @param correlationId an ID to help match responses to requests.
   * @param clientId the ID of the client.
   * @param error the {@link ServerErrorCode} for the request.
   */
  public AdminResponseWithContent(int correlationId, String clientId, ServerErrorCode error) {
    super(correlationId, clientId, error);
    content = null;
  }

  /**
   * Constructs an {@link AdminResponseWithContent}
   * @param correlationId an ID to help match responses to requests.
   * @param clientId the ID of the client.
   * @param error the {@link ServerErrorCode} for the request.
   * @param content the content to return back to client
   */
  public AdminResponseWithContent(int correlationId, String clientId, ServerErrorCode error, byte[] content) {
    super(correlationId, clientId, error);
    this.content = content;
  }

  /**
   * Reads from a stream and constructs an {@link AdminResponseWithContent}.
   * @param stream  the {@link DataInputStream} to read from.
   * @return {@link AdminResponseWithContent} that is deserialized from the {@code stream}.
   * @throws IOException if there is an I/O error reading from {@code stream}
   */
  public static AdminResponseWithContent readFrom(DataInputStream stream) throws IOException {
    AdminResponse adminResponse = AdminResponse.readFrom(stream);
    int contentLength = stream.readInt();
    byte[] content = null;
    if (contentLength != 0) {
      content = new byte[contentLength];
      stream.readFully(content);
    }
    return new AdminResponseWithContent(adminResponse.getCorrelationId(), adminResponse.getClientId(),
        adminResponse.getError(), content);
  }

  /**
   * Return the content.
   * @return
   */
  public byte[] getContent() {
    return content;
  }

  @Override
  public long sizeInBytes() {
    long size = super.sizeInBytes() + CONTENT_LENGTH_SIZE;
    if (content != null) {
      size += content.length;
    }
    return size;
  }

  @Override
  public void prepareBuffer() {
    super.prepareBuffer();
    bufferToSend.writeInt(content == null ? 0 : content.length);
    if (content != null && content.length != 0) {
      bufferToSend.writeBytes(content);
    }
  }

  @Override
  public String toString() {
    return "AdminResponseWithContent[ClientId=" + clientId + ", CorrelationId=" + correlationId + ", Type=" + type
        + ", ServerError=" + getError() + ", ContentLength=" + (content == null ? 0 : content.length) + "]";
  }
}
