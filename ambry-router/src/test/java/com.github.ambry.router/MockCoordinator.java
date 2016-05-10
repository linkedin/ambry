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
package com.github.ambry.router;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.ServerErrorCode;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.coordinator.Coordinator;
import com.github.ambry.coordinator.CoordinatorError;
import com.github.ambry.coordinator.CoordinatorException;
import com.github.ambry.messageformat.BlobOutput;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.utils.ByteBufferInputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * An implementation of {@link com.github.ambry.coordinator.Coordinator} for use in tests. Can be configured for custom
 * behavior to check for various scenarios.
 */
class MockCoordinator implements Coordinator {
  protected static String CHECKED_EXCEPTION_ON_OPERATION_START = "coordinator.checked.exception.on.operation.start";
  protected static String RUNTIME_EXCEPTION_ON_OPERATION_START = "coordinator.runtime.exception.on.operation.start";

  private final AtomicBoolean open = new AtomicBoolean(true);
  private final ClusterMap clusterMap;
  private final MockCluster cluster;
  private final VerifiableProperties verifiableProperties;

  /**
   * Creates an instance of MockCoordinator.
   * @param verifiableProperties properties map that defines the behavior of this instance.
   * @param clusterMap the cluster map to use.
   */
  public MockCoordinator(VerifiableProperties verifiableProperties, ClusterMap clusterMap) {
    this.verifiableProperties = verifiableProperties;
    this.clusterMap = clusterMap;
    cluster = new MockCluster(clusterMap);
  }

  @Override
  public String putBlob(BlobProperties blobProperties, ByteBuffer userMetadata, InputStream blobInputStream)
      throws CoordinatorException {
    if (!open.get()) {
      throw new IllegalArgumentException("Coordinator has been closed. Operation cannot be performed");
    } else if (verifiableProperties.getBoolean(CHECKED_EXCEPTION_ON_OPERATION_START, false)) {
      throw new CoordinatorException(CHECKED_EXCEPTION_ON_OPERATION_START, CoordinatorError.UnexpectedInternalError);
    } else if (verifiableProperties.getBoolean(RUNTIME_EXCEPTION_ON_OPERATION_START, false)) {
      throw new RuntimeException(RUNTIME_EXCEPTION_ON_OPERATION_START);
    }
    try {
      ByteBufferInputStream materializedBlobStream =
          new ByteBufferInputStream(blobInputStream, (int) blobProperties.getBlobSize());
      int index = new Random().nextInt(clusterMap.getWritablePartitionIds().size());
      PartitionId writablePartition = clusterMap.getWritablePartitionIds().get(index);
      BlobId blobId = new BlobId(writablePartition);
      ServerErrorCode error = ServerErrorCode.No_Error;
      for (ReplicaId replicaId : writablePartition.getReplicaIds()) {
        DataNodeId dataNodeId = replicaId.getDataNodeId();
        MockDataNode dataNode = cluster.getMockDataNode(dataNodeId.getHostname(), dataNodeId.getPort());
        BlobOutput blobOutput = new BlobOutput(blobProperties.getBlobSize(), materializedBlobStream.duplicate());
        Blob blob = new Blob(blobProperties, userMetadata, blobOutput);
        error = dataNode.put(blobId, blob);
        if (!ServerErrorCode.No_Error.equals(error)) {
          break;
        }
      }
      switch (error) {
        case No_Error:
          break;
        case Disk_Unavailable:
          throw new CoordinatorException(error.toString(), CoordinatorError.AmbryUnavailable);
        default:
          throw new CoordinatorException(error.toString(), CoordinatorError.UnexpectedInternalError);
      }
      return blobId.getID();
    } catch (IOException e) {
      throw new CoordinatorException(e, CoordinatorError.UnexpectedInternalError);
    }
  }

  @Override
  public void deleteBlob(String id)
      throws CoordinatorException {
    if (!open.get()) {
      throw new IllegalArgumentException("Coordinator has been closed. Operation cannot be performed");
    } else if (verifiableProperties.getBoolean(CHECKED_EXCEPTION_ON_OPERATION_START, false)) {
      throw new CoordinatorException(CHECKED_EXCEPTION_ON_OPERATION_START, CoordinatorError.UnexpectedInternalError);
    } else if (verifiableProperties.getBoolean(RUNTIME_EXCEPTION_ON_OPERATION_START, false)) {
      throw new RuntimeException(RUNTIME_EXCEPTION_ON_OPERATION_START);
    }
    try {
      BlobId blobId = new BlobId(id, clusterMap);
      ServerErrorCode error = ServerErrorCode.No_Error;
      for (ReplicaId replicaId : blobId.getPartition().getReplicaIds()) {
        DataNodeId dataNodeId = replicaId.getDataNodeId();
        MockDataNode dataNode = cluster.getMockDataNode(dataNodeId.getHostname(), dataNodeId.getPort());
        error = dataNode.delete(blobId);
        if (!(ServerErrorCode.No_Error.equals(error) || ServerErrorCode.Blob_Deleted.equals(error))) {
          break;
        }
      }
      switch (error) {
        case No_Error:
        case Blob_Deleted:
          break;
        case Blob_Not_Found:
        case Partition_Unknown:
          throw new CoordinatorException(error.toString(), CoordinatorError.BlobDoesNotExist);
        case Blob_Expired:
          throw new CoordinatorException(error.toString(), CoordinatorError.BlobExpired);
        case Disk_Unavailable:
          throw new CoordinatorException(error.toString(), CoordinatorError.AmbryUnavailable);
        case IO_Error:
        default:
          throw new CoordinatorException(error.toString(), CoordinatorError.UnexpectedInternalError);
      }
    } catch (IOException e) {
      throw new CoordinatorException(e, CoordinatorError.UnexpectedInternalError);
    }
  }

  @Override
  public BlobProperties getBlobProperties(String id)
      throws CoordinatorException {
    if (!open.get()) {
      throw new IllegalArgumentException("Coordinator has been closed. Operation cannot be performed");
    } else if (verifiableProperties.getBoolean(CHECKED_EXCEPTION_ON_OPERATION_START, false)) {
      throw new CoordinatorException(CHECKED_EXCEPTION_ON_OPERATION_START, CoordinatorError.UnexpectedInternalError);
    } else if (verifiableProperties.getBoolean(RUNTIME_EXCEPTION_ON_OPERATION_START, false)) {
      throw new RuntimeException(RUNTIME_EXCEPTION_ON_OPERATION_START);
    }
    try {
      BlobId blobId = new BlobId(id, clusterMap);
      int index = new Random().nextInt(blobId.getPartition().getReplicaIds().size());
      ReplicaId replicaToGetFrom = blobId.getPartition().getReplicaIds().get(index);
      DataNodeId dataNodeId = replicaToGetFrom.getDataNodeId();
      MockDataNode dataNode = cluster.getMockDataNode(dataNodeId.getHostname(), dataNodeId.getPort());
      MockDataNode.BlobPropertiesAndError bpae = dataNode.getBlobProperties(blobId);
      handleGetError(bpae.getError());
      return bpae.getBlobProperties();
    } catch (IOException e) {
      throw new CoordinatorException(e, CoordinatorError.UnexpectedInternalError);
    }
  }

  @Override
  public ByteBuffer getBlobUserMetadata(String id)
      throws CoordinatorException {
    if (!open.get()) {
      throw new IllegalArgumentException("Coordinator has been closed. Operation cannot be performed");
    } else if (verifiableProperties.getBoolean(CHECKED_EXCEPTION_ON_OPERATION_START, false)) {
      throw new CoordinatorException(CHECKED_EXCEPTION_ON_OPERATION_START, CoordinatorError.UnexpectedInternalError);
    } else if (verifiableProperties.getBoolean(RUNTIME_EXCEPTION_ON_OPERATION_START, false)) {
      throw new RuntimeException(RUNTIME_EXCEPTION_ON_OPERATION_START);
    }
    try {
      BlobId blobId = new BlobId(id, clusterMap);
      int index = new Random().nextInt(blobId.getPartition().getReplicaIds().size());
      ReplicaId replicaToGetFrom = blobId.getPartition().getReplicaIds().get(index);
      DataNodeId dataNodeId = replicaToGetFrom.getDataNodeId();
      MockDataNode dataNode = cluster.getMockDataNode(dataNodeId.getHostname(), dataNodeId.getPort());
      MockDataNode.UserMetadataAndError umae = dataNode.getUserMetadata(blobId);
      handleGetError(umae.getError());
      return umae.getUserMetadata();
    } catch (IOException e) {
      throw new CoordinatorException(e, CoordinatorError.UnexpectedInternalError);
    }
  }

  @Override
  public BlobOutput getBlob(String id)
      throws CoordinatorException {
    if (!open.get()) {
      throw new IllegalArgumentException("Coordinator has been closed. Operation cannot be performed");
    } else if (verifiableProperties.getBoolean(CHECKED_EXCEPTION_ON_OPERATION_START, false)) {
      throw new CoordinatorException(CHECKED_EXCEPTION_ON_OPERATION_START, CoordinatorError.UnexpectedInternalError);
    } else if (verifiableProperties.getBoolean(RUNTIME_EXCEPTION_ON_OPERATION_START, false)) {
      throw new RuntimeException(RUNTIME_EXCEPTION_ON_OPERATION_START);
    }
    try {
      BlobId blobId = new BlobId(id, clusterMap);
      int index = new Random().nextInt(blobId.getPartition().getReplicaIds().size());
      ReplicaId replicaToGetFrom = blobId.getPartition().getReplicaIds().get(index);
      DataNodeId dataNodeId = replicaToGetFrom.getDataNodeId();
      MockDataNode dataNode = cluster.getMockDataNode(dataNodeId.getHostname(), dataNodeId.getPort());
      MockDataNode.BlobOutputAndError boae = dataNode.getData(blobId);
      handleGetError(boae.getError());
      return boae.getBlobOutput();
    } catch (IOException e) {
      throw new CoordinatorException(e, CoordinatorError.UnexpectedInternalError);
    }
  }

  @Override
  public void close()
      throws IOException {
    if (verifiableProperties.getBoolean(CHECKED_EXCEPTION_ON_OPERATION_START, false)) {
      throw new IOException(CHECKED_EXCEPTION_ON_OPERATION_START);
    }
    open.set(false);
  }

  /**
   * Converts a {@link ServerErrorCode} encountered during {@link #getBlob(String)}, {@link #getBlobProperties(String)}
   * and {@link #getBlobUserMetadata(String)} to a {@link CoordinatorException} (if required).
   * @param error the {@link ServerErrorCode} that needs to be converted to a {@link CoordinatorException}.
   * @throws CoordinatorException the {@link CoordinatorException} that matches the {@code error}.
   */
  private void handleGetError(ServerErrorCode error)
      throws CoordinatorException {
    switch (error) {
      case No_Error:
        break;
      case Blob_Not_Found:
      case Partition_Unknown:
        throw new CoordinatorException(error.toString(), CoordinatorError.BlobDoesNotExist);
      case Blob_Deleted:
        throw new CoordinatorException(error.toString(), CoordinatorError.BlobDeleted);
      case Blob_Expired:
        throw new CoordinatorException(error.toString(), CoordinatorError.BlobExpired);
      case Disk_Unavailable:
        throw new CoordinatorException(error.toString(), CoordinatorError.AmbryUnavailable);
      case IO_Error:
      case Data_Corrupt:
      default:
        throw new CoordinatorException(error.toString(), CoordinatorError.UnexpectedInternalError);
    }
  }
}

/**
 * A cluster with {@link MockDataNode}s.
 */
class MockCluster {
  private Map<DataNodeId, MockDataNode> mockDataNodes;
  private ClusterMap clustermap;

  public MockCluster(ClusterMap clusterMap) {
    this.mockDataNodes = new HashMap<DataNodeId, MockDataNode>();
    this.clustermap = clusterMap;
  }

  public synchronized MockDataNode getMockDataNode(String host, int port) {
    DataNodeId dataNodeId = clustermap.getDataNodeId(host, port);
    if (!mockDataNodes.containsKey(dataNodeId)) {
      mockDataNodes.put(dataNodeId, new MockDataNode(dataNodeId));
    }
    return mockDataNodes.get(dataNodeId);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (MockDataNode mockDataNode : mockDataNodes.values()) {
      sb.append(mockDataNode);
    }
    return sb.toString();
  }
}

/**
 * A data node that stores all data in memory.
 */
class MockDataNode {
  private DataNodeId dataNodeId;

  private Map<BlobId, MaterializedBlob> blobs;
  private Set<BlobId> deletedBlobs;

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
    if (blobs.containsKey(blobId)) {
      return ServerErrorCode.Unknown_Error;
    }
    blobs.put(blobId, new MaterializedBlob(blob));
    return ServerErrorCode.No_Error;
  }

  class BlobPropertiesAndError {
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

  class UserMetadataAndError {
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

  class BlobOutputAndError {
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

  synchronized BlobOutputAndError getData(BlobId blobId) {
    if (deletedBlobs.contains(blobId)) {
      return new BlobOutputAndError(null, ServerErrorCode.Blob_Deleted);
    }

    if (!blobs.containsKey(blobId)) {
      return new BlobOutputAndError(null, ServerErrorCode.Blob_Not_Found);
    }

    return new BlobOutputAndError(blobs.get(blobId).getBlobOutput(), ServerErrorCode.No_Error);
  }

  public synchronized ServerErrorCode delete(BlobId blobId) {
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
}

/**
 * Blob stored in {@link MockDataNode}. Blob consists of properties, user metadata, and data.
 */
class Blob {
  private final BlobProperties blobProperties;
  private final ByteBuffer userMetadata;
  private final BlobOutput blobOutput;

  public Blob(BlobProperties blobProperties, ByteBuffer userMetadata, BlobOutput blobOutput) {
    this.blobProperties = blobProperties;
    this.userMetadata = userMetadata;
    this.blobOutput = blobOutput;
  }

  public BlobProperties getBlobProperties() {
    return blobProperties;
  }

  public ByteBuffer getUserMetadata() {
    return userMetadata;
  }

  public BlobOutput getBlobOutput() {
    return blobOutput;
  }
}

