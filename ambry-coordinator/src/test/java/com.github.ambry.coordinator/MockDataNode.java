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
package com.github.ambry.coordinator;

import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.ServerErrorCode;
import com.github.ambry.messageformat.BlobOutput;
import com.github.ambry.messageformat.BlobProperties;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


/**
 *
 */
public class MockDataNode {
  private DataNodeId dataNodeId;

  private Map<BlobId, MaterializedBlob> blobs;
  private Set<BlobId> deletedBlobs;
  private ServerErrorCode deleteErrorCode;
  private ServerErrorCode getErrorCode;
  private ServerErrorCode putErrorCode;

  public class MaterializedBlob extends Blob {
    final ByteBuffer materializedBlobOutput;

    public MaterializedBlob(Blob blob)
        throws IOException {
      super(blob.getBlobProperties(), blob.getUserMetadata(), blob.getBlobOutput());

      BlobOutput blobOutput = blob.getBlobOutput();
      byte[] blobOutputBytes = new byte[(int) blobOutput.getSize()];
      new DataInputStream(blobOutput.getStream()).readFully(blobOutputBytes);
      this.materializedBlobOutput = ByteBuffer.allocate((int) blobOutput.getSize());
      materializedBlobOutput.put(blobOutputBytes);
    }

    @Override
    public BlobOutput getBlobOutput() {
      return new BlobOutput(super.getBlobOutput().getSize(), new ByteArrayInputStream(materializedBlobOutput.array()));
    }
  }

  public MockDataNode(DataNodeId dataNodeId) {
    this.dataNodeId = dataNodeId;

    blobs = new HashMap<BlobId, MaterializedBlob>();
    deletedBlobs = new HashSet<BlobId>();
  }

  public synchronized ServerErrorCode put(BlobId blobId, Blob blob)
      throws IOException {
    if (putErrorCode != null) {
      return putErrorCode;
    }
    if (blobs.containsKey(blobId)) {
      return ServerErrorCode.Unknown_Error;
    }
    blobs.put(blobId, new MaterializedBlob(blob));
    return ServerErrorCode.No_Error;
  }

  public class BlobPropertiesAndError {
    private BlobProperties blobProperties;
    private ServerErrorCode error;

    BlobPropertiesAndError(BlobProperties blobProperties, ServerErrorCode error) {
      this.blobProperties = blobProperties;
      this.error = error;
    }

    public BlobProperties getBlobProperties() {
      return blobProperties;
    }

    public ServerErrorCode getError() {
      return error;
    }
  }

  public synchronized BlobPropertiesAndError getBlobProperties(BlobId blobId) {
    if (deletedBlobs.contains(blobId)) {
      return new BlobPropertiesAndError(null, ServerErrorCode.Blob_Deleted);
    }

    if (!blobs.containsKey(blobId)) {
      return new BlobPropertiesAndError(null, ServerErrorCode.Blob_Not_Found);
    }

    return new BlobPropertiesAndError(blobs.get(blobId).getBlobProperties(), ServerErrorCode.No_Error);
  }

  public class UserMetadataAndError {
    private ByteBuffer userMetadata;
    private ServerErrorCode error;

    UserMetadataAndError(ByteBuffer userMetadata, ServerErrorCode error) {
      this.userMetadata = userMetadata;
      this.error = error;
    }

    public ByteBuffer getUserMetadata() {
      return userMetadata;
    }

    public ServerErrorCode getError() {
      return error;
    }
  }

  public synchronized UserMetadataAndError getUserMetadata(BlobId blobId) {
    if (deletedBlobs.contains(blobId)) {
      return new UserMetadataAndError(null, ServerErrorCode.Blob_Deleted);
    }

    if (!blobs.containsKey(blobId)) {
      return new UserMetadataAndError(null, ServerErrorCode.Blob_Not_Found);
    }

    return new UserMetadataAndError(blobs.get(blobId).getUserMetadata(), ServerErrorCode.No_Error);
  }

  public class BlobOutputAndError {
    private BlobOutput blobOutput;
    private ServerErrorCode error;

    BlobOutputAndError(BlobOutput blobOutput, ServerErrorCode error) {
      this.blobOutput = blobOutput;
      this.error = error;
    }

    public BlobOutput getBlobOutput() {
      return blobOutput;
    }

    public ServerErrorCode getError() {
      return error;
    }
  }

  public synchronized BlobOutputAndError getData(BlobId blobId) {
    if (getErrorCode != null) {
      return new BlobOutputAndError(null, getErrorCode);
    }
    if (deletedBlobs.contains(blobId)) {
      return new BlobOutputAndError(null, ServerErrorCode.Blob_Deleted);
    }

    if (!blobs.containsKey(blobId)) {
      return new BlobOutputAndError(null, ServerErrorCode.Blob_Not_Found);
    }

    return new BlobOutputAndError(blobs.get(blobId).getBlobOutput(), ServerErrorCode.No_Error);
  }

  public synchronized ServerErrorCode delete(BlobId blobId) {
    if (deleteErrorCode != null) {
      return deleteErrorCode;
    }
    if (deletedBlobs.contains(blobId)) {
      return ServerErrorCode.Blob_Deleted;
    } else if (!blobs.containsKey(blobId)) {
      return ServerErrorCode.Blob_Not_Found;
    } else {
      blobs.remove(blobId);
      deletedBlobs.add(blobId);
      return ServerErrorCode.No_Error;
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(dataNodeId).append(System.getProperty("line.separator"));
    sb.append("put blobs").append(System.getProperty("line.separator"));
    for (Map.Entry<BlobId, MaterializedBlob> entry : blobs.entrySet()) {
      sb.append("\t").append(entry.getKey()).append(" : ");
      Blob blob = entry.getValue();
      sb.append(blob.getBlobProperties().getBlobSize()).append(" / ");
      sb.append(blob.getUserMetadata().capacity()).append(" / ");
      sb.append(blob.getBlobOutput().getSize()).append(System.getProperty("line.separator"));
    }
    sb.append("deleted blobs").append(System.getProperty("line.separator"));
    for (BlobId blobId : deletedBlobs) {
      sb.append("\t").append(blobId).append(System.getProperty("line.separator"));
    }
    return sb.toString();
  }

  public void setDeleteException(ServerErrorCode serverErrorCode) {
    this.deleteErrorCode = serverErrorCode;
  }

  public void setGetException(ServerErrorCode serverErrorCode) {
    this.getErrorCode = serverErrorCode;
  }

  public void setPutException(ServerErrorCode serverErrorCode) {
    this.putErrorCode = serverErrorCode;
  }
}
