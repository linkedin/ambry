/*
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
package com.github.ambry.protocol;

import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;


/**
 * An admin request used to control replication behavior (enable/disable)
 * <p/>
 * This request can be used to control replication (at a single storage node) of particular partitions from particular
 * datacenters (i.e. replication that adds data locally on the given storage node).
 */
public class ReplicationControlAdminRequest extends AdminRequest {
  private static final short VERSION_V1 = 1;

  private final List<String> origins;
  private final boolean enable;
  private final long sizeInBytes;

  /**
   * Reads from a stream and constructs a {@link ReplicationControlAdminRequest}.
   * @param stream the stream to read from
   * @param adminRequest the {@link AdminRequest} that contains some necessary headers.
   * @return the {@link ReplicationControlAdminRequest} constructed from the {@code stream}.
   * @throws IOException if there is any problem reading from the stream
   */
  public static ReplicationControlAdminRequest readFrom(DataInputStream stream, AdminRequest adminRequest)
      throws IOException {
    Short versionId = stream.readShort();
    if (!versionId.equals(VERSION_V1)) {
      throw new IllegalStateException("Unrecognized version for ReplicationControlAdminRequest: " + versionId);
    }
    int listSize = stream.readInt();
    List<String> origins = new ArrayList<>();
    for (int i = 0; i < listSize; i++) {
      origins.add(Utils.readIntString(stream, StandardCharsets.UTF_8));
    }
    boolean enable = stream.readByte() == 1;
    return new ReplicationControlAdminRequest(origins, enable, adminRequest);
  }

  /**
   * Construct a ReplicationControlAdminRequest
   * @param origins the list of datacenters from which replication should be enabled/disabled.
   * @param enable enable/disable flag ({@code true} to enable).
   * @param adminRequest the {@link AdminRequest} that contains common admin request related information.
   */
  public ReplicationControlAdminRequest(List<String> origins, boolean enable, AdminRequest adminRequest) {
    super(AdminRequestOrResponseType.ReplicationControl, adminRequest.getPartitionId(), adminRequest.getCorrelationId(),
        adminRequest.getClientId());
    this.origins = origins;
    this.enable = enable;
    sizeInBytes = computeSizeInBytes();
  }

  /**
   * @return the list of datacenters from which replication should be enabled/disabled.
   */
  public List<String> getOrigins() {
    return origins;
  }

  /**
   * @return if replication from {@link #getOrigins()} needs to enabled ({@code true}) or disabled ({@code false}).
   */
  public boolean shouldEnable() {
    return enable;
  }

  @Override
  public long sizeInBytes() {
    return sizeInBytes;
  }

  @Override
  public String toString() {
    return "ReplicationControlAdminRequest[ClientId=" + clientId + ", CorrelationId=" + correlationId + ", Origins="
        + origins + ", PartitionId=" + getPartitionId() + "]";
  }

  @Override
  protected void serializeIntoBuffer() {
    super.serializeIntoBuffer();
    bufferToSend.putShort(VERSION_V1);
    bufferToSend.putInt(origins.size());
    for (String origin : origins) {
      Utils.serializeString(bufferToSend, origin, StandardCharsets.UTF_8);
    }
    bufferToSend.put(enable ? (byte) 1 : 0);
  }

  private long computeSizeInBytes() {
    // parent size + version size + list length size
    long size = super.sizeInBytes() + Short.BYTES + Integer.BYTES;
    for (String origin : origins) {
      // size of length field
      size += Integer.BYTES;
      // size of the byte representation of the string
      size += origin.getBytes(StandardCharsets.UTF_8).length;
    }
    // enable flag size
    size += Byte.BYTES;
    return size;
  }
}
