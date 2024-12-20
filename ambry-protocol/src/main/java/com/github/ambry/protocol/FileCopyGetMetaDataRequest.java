/**
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
package com.github.ambry.protocol;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.Charset;

public class FileCopyGetMetaDataRequest extends RequestOrResponse{
  private PartitionId partitionId;
  private String hostName;
  private static final short File_Metadata_Request_Version_V1 = 1;
  private static final int HostName_Field_Size_In_Bytes = 4;

  public FileCopyGetMetaDataRequest(short versionId, int correlationId, String clientId,
      PartitionId partitionId, String hostName) {
    super(RequestOrResponseType.FileCopyGetMetaDataRequest, versionId, correlationId, clientId);
    if (partitionId == null) {
      throw new IllegalArgumentException("Partition cannot be null");
    }
    if (hostName.isEmpty()){
      throw new IllegalArgumentException("Host Name cannot be null");
    }
    this.partitionId = partitionId;
    this.hostName = hostName;
  }

  public String getHostName() {
    return hostName;
  }

  public PartitionId getPartitionId() {
    return partitionId;
  }

  protected static FileCopyGetMetaDataRequest readFrom(DataInputStream stream, ClusterMap clusterMap) throws IOException {
    Short versionId = stream.readShort();
    validateVersion(versionId);
    int correlationId = stream.readInt();
    String clientId = Utils.readIntString(stream);
    String hostName = Utils.readIntString(stream);
    PartitionId partitionId = clusterMap.getPartitionIdFromStream(stream);
    return new FileCopyGetMetaDataRequest(versionId, correlationId, clientId, partitionId, hostName);
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("FileMetaDataRequest[").append("PartitionId=").append(partitionId).append(", HostName=").append(hostName)
        .append("]");
    return sb.toString();
  }

  public long sizeInBytes() {
    return super.sizeInBytes() + HostName_Field_Size_In_Bytes + hostName.length() + partitionId.getBytes().length;
  }

  protected void prepareBuffer() {
    super.prepareBuffer();
    Utils.serializeString(bufferToSend, hostName, Charset.defaultCharset());
    bufferToSend.writeBytes(partitionId.getBytes());
  }

  static void validateVersion(short version) {
    if (version != File_Metadata_Request_Version_V1) {
      throw new IllegalArgumentException("Unknown version for FileMetadataRequest: " + version);
    }
  }
}