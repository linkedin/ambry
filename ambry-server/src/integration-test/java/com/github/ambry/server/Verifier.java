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
package com.github.ambry.server;

import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockDataNodeId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.BlobIdFactory;
import com.github.ambry.messageformat.BlobAll;
import com.github.ambry.messageformat.BlobData;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.MessageFormatException;
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.messageformat.MessageFormatRecord;
import com.github.ambry.network.ConnectedChannel;
import com.github.ambry.network.ConnectionPool;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import com.github.ambry.notification.UpdateType;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.protocol.GetRequest;
import com.github.ambry.protocol.GetResponse;
import com.github.ambry.protocol.PartitionRequestInfo;
import com.github.ambry.utils.Utils;
import io.netty.buffer.ByteBuf;
import java.io.DataInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Verifier thread to get blob from ambry and check with the original blob
 */
class Verifier implements Runnable {
  private BlockingQueue<Payload> payloadQueue;
  private CountDownLatch completedLatch;
  private AtomicInteger totalRequests;
  private AtomicInteger requestsVerified;
  private MockClusterMap clusterMap;
  private AtomicBoolean cancelTest;
  private PortType portType;
  private ConnectionPool connectionPool;
  private MockNotificationSystem notificationSystem;

  Verifier(BlockingQueue<Payload> payloadQueue, CountDownLatch completedLatch, AtomicInteger totalRequests,
      AtomicInteger requestsVerified, MockClusterMap clusterMap, AtomicBoolean cancelTest, PortType portType,
      ConnectionPool connectionPool, MockNotificationSystem notificationSystem) {
    this.payloadQueue = payloadQueue;
    this.completedLatch = completedLatch;
    this.totalRequests = totalRequests;
    this.requestsVerified = requestsVerified;
    this.clusterMap = clusterMap;
    this.cancelTest = cancelTest;
    this.portType = portType;
    this.connectionPool = connectionPool;
    this.notificationSystem = notificationSystem;
  }

  @Override
  public void run() {
    try {
      List<PartitionRequestInfo> partitionRequestInfoList = new ArrayList<PartitionRequestInfo>();
      while (requestsVerified.get() != totalRequests.get() && !cancelTest.get()) {
        Payload payload = payloadQueue.poll(1000, TimeUnit.MILLISECONDS);
        if (payload != null) {
          notificationSystem.awaitBlobCreations(payload.blobId);
          for (MockDataNodeId dataNodeId : clusterMap.getDataNodes()) {
            ConnectedChannel channel1 = null;
            try {
              BlobId blobId = new BlobId(payload.blobId, clusterMap);
              Port port =
                  new Port(portType == PortType.PLAINTEXT ? dataNodeId.getPort() : dataNodeId.getSSLPort(), portType);
              channel1 = connectionPool.checkOutConnection("localhost", port, 10000);
              ArrayList<BlobId> ids = new ArrayList<BlobId>();
              ids.add(blobId);
              partitionRequestInfoList.clear();
              PartitionRequestInfo partitionRequestInfo = new PartitionRequestInfo(ids.get(0).getPartition(), ids);
              partitionRequestInfoList.add(partitionRequestInfo);
              GetRequest getRequest =
                  new GetRequest(1, "clientid2", MessageFormatFlags.BlobProperties, partitionRequestInfoList,
                      GetOption.None);
              channel1.send(getRequest);
              InputStream stream = channel1.receive().getInputStream();
              GetResponse resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
              if (resp.getError() != ServerErrorCode.No_Error) {
                System.out.println(dataNodeId.getHostname() + " " + dataNodeId.getPort() + " " + resp.getError());
                throw new IllegalStateException();
              } else {
                try {
                  BlobProperties propertyOutput = MessageFormatRecord.deserializeBlobProperties(resp.getInputStream());
                  if (propertyOutput.getBlobSize() != payload.blobProperties.getBlobSize()) {
                    String exceptionMsg =
                        "blob size not matching " + " expected " + payload.blobProperties.getBlobSize() + " actual "
                            + propertyOutput.getBlobSize();
                    System.out.println(exceptionMsg);
                    throw new IllegalStateException(exceptionMsg);
                  }
                  if (!propertyOutput.getServiceId().equals(payload.blobProperties.getServiceId())) {
                    String exceptionMsg =
                        "service id not matching " + " expected " + payload.blobProperties.getServiceId() + " actual "
                            + propertyOutput.getBlobSize();
                    System.out.println(exceptionMsg);
                    throw new IllegalStateException(exceptionMsg);
                  }
                  if (propertyOutput.getAccountId() != payload.blobProperties.getAccountId()) {
                    String exceptionMsg =
                        "accountid not matching " + " expected " + payload.blobProperties.getAccountId() + " actual "
                            + propertyOutput.getAccountId();
                    System.out.println(exceptionMsg);
                    throw new IllegalStateException(exceptionMsg);
                  }
                  if (propertyOutput.getContainerId() != payload.blobProperties.getContainerId()) {
                    String exceptionMsg =
                        "containerId not matching " + " expected " + payload.blobProperties.getContainerId()
                            + " actual " + propertyOutput.getContainerId();
                    System.out.println(exceptionMsg);
                    throw new IllegalStateException(exceptionMsg);
                  }
                  if (propertyOutput.isEncrypted() != payload.blobProperties.isEncrypted()) {
                    String exceptionMsg =
                        "IsEncrypted not matching " + " expected " + payload.blobProperties.isEncrypted() + " actual "
                            + propertyOutput.isEncrypted();
                    System.out.println(exceptionMsg);
                    throw new IllegalStateException(exceptionMsg);
                  }
                  long actualExpiryTimeMs =
                      resp.getPartitionResponseInfoList().get(0).getMessageInfoList().get(0).getExpirationTimeInMs();
                  checkExpiryTimeMatch(payload, actualExpiryTimeMs, "messageinfo in blobproperty");
                } catch (MessageFormatException e) {
                  e.printStackTrace();
                  throw new IllegalStateException(e);
                }
              }

              // get user metadata
              ids.clear();
              ids.add(blobId);
              partitionRequestInfoList.clear();
              partitionRequestInfo = new PartitionRequestInfo(ids.get(0).getPartition(), ids);
              partitionRequestInfoList.add(partitionRequestInfo);
              getRequest = new GetRequest(1, "clientid2", MessageFormatFlags.BlobUserMetadata, partitionRequestInfoList,
                  GetOption.None);
              channel1.send(getRequest);
              stream = channel1.receive().getInputStream();
              resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
              if (resp.getError() != ServerErrorCode.No_Error) {
                System.out.println("Error after get user metadata " + resp.getError());
                throw new IllegalStateException();
              } else {
                try {
                  ByteBuffer userMetadataOutput = MessageFormatRecord.deserializeUserMetadata(resp.getInputStream());
                  if (userMetadataOutput.compareTo(ByteBuffer.wrap(payload.metadata)) != 0) {
                    throw new IllegalStateException();
                  }
                  long actualExpiryTimeMs =
                      resp.getPartitionResponseInfoList().get(0).getMessageInfoList().get(0).getExpirationTimeInMs();
                  checkExpiryTimeMatch(payload, actualExpiryTimeMs, "messageinfo in usermetadatga");
                } catch (MessageFormatException e) {
                  e.printStackTrace();
                  throw new IllegalStateException();
                }
              }

              // get blob
              ids.clear();
              ids.add(blobId);
              partitionRequestInfoList.clear();
              partitionRequestInfo = new PartitionRequestInfo(ids.get(0).getPartition(), ids);
              partitionRequestInfoList.add(partitionRequestInfo);
              getRequest =
                  new GetRequest(1, "clientid2", MessageFormatFlags.Blob, partitionRequestInfoList, GetOption.None);
              channel1.send(getRequest);
              stream = channel1.receive().getInputStream();
              resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
              //System.out.println("response from get " + resp.getError());
              if (resp.getError() != ServerErrorCode.No_Error) {
                System.out.println("Error after get blob " + resp.getError());
                throw new IllegalStateException();
              } else {
                try {
                  BlobData blobData = MessageFormatRecord.deserializeBlob(resp.getInputStream());
                  byte[] blobout = new byte[(int) blobData.getSize()];
                  ByteBuf buffer = blobData.content();
                  try {
                    buffer.readBytes(blobout);
                  } finally {
                    buffer.release();
                  }
                  if (ByteBuffer.wrap(blobout).compareTo(ByteBuffer.wrap(payload.blob)) != 0) {
                    throw new IllegalStateException();
                  }
                  long actualExpiryTimeMs =
                      resp.getPartitionResponseInfoList().get(0).getMessageInfoList().get(0).getExpirationTimeInMs();
                  checkExpiryTimeMatch(payload, actualExpiryTimeMs, "messageinfo in blobdata");
                } catch (MessageFormatException e) {
                  e.printStackTrace();
                  throw new IllegalStateException();
                }
              }

              // get blob all
              getRequest =
                  new GetRequest(1, "clientid2", MessageFormatFlags.All, partitionRequestInfoList, GetOption.None);
              channel1.send(getRequest);
              stream = channel1.receive().getInputStream();
              resp = GetResponse.readFrom(new DataInputStream(stream), clusterMap);
              if (resp.getError() != ServerErrorCode.No_Error) {
                System.out.println("Error after get blob " + resp.getError());
                throw new IllegalStateException();
              } else {
                try {
                  BlobAll blobAll =
                      MessageFormatRecord.deserializeBlobAll(resp.getInputStream(), new BlobIdFactory(clusterMap));
                  byte[] blobout = new byte[(int) blobAll.getBlobData().getSize()];
                  ByteBuf buffer = blobAll.getBlobData().content();
                  try {
                    buffer.readBytes(blobout);
                  } finally {
                    buffer.release();
                  }
                  if (ByteBuffer.wrap(blobout).compareTo(ByteBuffer.wrap(payload.blob)) != 0) {
                    throw new IllegalStateException();
                  }
                  long actualExpiryTimeMs =
                      resp.getPartitionResponseInfoList().get(0).getMessageInfoList().get(0).getExpirationTimeInMs();
                  checkExpiryTimeMatch(payload, actualExpiryTimeMs, "messageinfo in bloball");
                } catch (MessageFormatException e) {
                  e.printStackTrace();
                  throw new IllegalStateException();
                }
              }

              if (payload.blobProperties.getTimeToLiveInSeconds() != Utils.Infinite_Time) {
                // ttl update, check and wait for replication
                ServerTestUtil.updateBlobTtl(channel1, new BlobId(payload.blobId, clusterMap));
                ServerTestUtil.checkTtlUpdateStatus(channel1, clusterMap, new BlobIdFactory(clusterMap), blobId, payload.blob, true, Utils.Infinite_Time);
                notificationSystem.awaitBlobUpdates(payload.blobId, UpdateType.TTL_UPDATE);
                BlobProperties old = payload.blobProperties;
                payload.blobProperties = new BlobProperties(old.getBlobSize(), old.getServiceId(), old.getOwnerId(), old.getContentType(),
                    old.isEncrypted(), Utils.Infinite_Time, old.getCreationTimeInMs(), old.getAccountId(), old.getContainerId(), old.isEncrypted(), old.getExternalAssetTag());
              }
            } catch (Exception e) {
              if (channel1 != null) {
                connectionPool.destroyConnection(channel1);
                channel1 = null;
              }
            } finally {
              if (channel1 != null) {
                connectionPool.checkInConnection(channel1);
                channel1 = null;
              }
            }
          }
          requestsVerified.incrementAndGet();
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      cancelTest.set(true);
    } finally {
      completedLatch.countDown();
    }
  }

  /**
   * Checks that the actual expiry time matches what is in the payload.
   * @param payload the payload that was provided to the PUT
   * @param actualExpiryTimeMs the actual expiry time received
   * @throws IllegalStateException if the times don't match
   */
  private void checkExpiryTimeMatch(Payload payload, long actualExpiryTimeMs, String context) {
    long expectedExpiryTimeMs = ServerTestUtil.getExpiryTimeMs(payload.blobProperties);
    if (actualExpiryTimeMs != expectedExpiryTimeMs) {
      String exceptionMsg =
          "Expiry time (props) not matching in context " + context + " expected " + expectedExpiryTimeMs + " actual "
              + actualExpiryTimeMs;
      System.out.println(exceptionMsg);
      throw new IllegalStateException(exceptionMsg);
    }
  }
}

