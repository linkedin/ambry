/*
 * Copyright 2026 LinkedIn Corp. All rights reserved.
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

import java.io.DataInputStream;
import java.io.IOException;


/**
 * Admin request that reads the current replication-priority snapshot for a single storage node.
 * Carries no additional body fields beyond the {@link AdminRequest} parent — the server returns
 * every priority entry held in memory at handler-invocation time.
 */
public class ListReplicationPriorityAdminRequest extends AdminRequest {
  private static final short VERSION_V1 = 1;

  private final long sizeInBytes;

  /**
   * Reads from a stream and constructs a {@link ListReplicationPriorityAdminRequest}.
   * @param stream the stream to read from
   * @param adminRequest the {@link AdminRequest} containing common header fields
   * @return the {@link ListReplicationPriorityAdminRequest} constructed from {@code stream}
   * @throws IOException if there is any problem reading from the stream
   */
  public static ListReplicationPriorityAdminRequest readFrom(DataInputStream stream, AdminRequest adminRequest)
      throws IOException {
    Short versionId = stream.readShort();
    if (!versionId.equals(VERSION_V1)) {
      throw new IllegalStateException("Unrecognized version for ListReplicationPriorityAdminRequest: " + versionId);
    }
    return new ListReplicationPriorityAdminRequest(adminRequest);
  }

  public ListReplicationPriorityAdminRequest(AdminRequest adminRequest) {
    super(AdminRequestOrResponseType.ListReplicationPriority, adminRequest.getPartitionId(),
        adminRequest.getCorrelationId(), adminRequest.getClientId());
    // parent size + version
    sizeInBytes = super.sizeInBytes() + Short.BYTES;
  }

  @Override
  public long sizeInBytes() {
    return sizeInBytes;
  }

  @Override
  public String toString() {
    return "ListReplicationPriorityAdminRequest[ClientId=" + clientId + ", CorrelationId=" + correlationId + "]";
  }

  @Override
  protected void prepareBuffer() {
    super.prepareBuffer();
    bufferToSend.writeShort(VERSION_V1);
  }
}
