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
import com.github.ambry.commons.ServerErrorCode;
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
import com.github.ambry.protocol.GetOption;
import com.github.ambry.protocol.GetRequest;
import com.github.ambry.protocol.GetResponse;
import com.github.ambry.protocol.PartitionRequestInfo;
import java.io.DataInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
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

  public Verifier(BlockingQueue<Payload> payloadQueue, CountDownLatch completedLatch, AtomicInteger totalRequests,
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
      ArrayList<PartitionRequestInfo> partitionRequestInfoList = new ArrayList<PartitionRequestInfo>();
      while (requestsVerified.get() != totalRequests.get() && !cancelTest.get()) {
        Payload payload = payloadQueue.poll(1000, TimeUnit.MILLISECONDS);
        if (payload != null) {
          notificationSystem.awaitBlobCreations(payload.blobId);
          for (MockDataNodeId dataNodeId : clusterMap.getDataNodes()) {
            ConnectedChannel channel1 = null;
            try {
              Port port =
                  new Port(portType == PortType.PLAINTEXT ? dataNodeId.getPort() : dataNodeId.getSSLPort(), portType);
              channel1 = connectionPool.checkOutConnection("localhost", port, 10000);
              ArrayList<BlobId> ids = new ArrayList<BlobId>();
              ids.add(new BlobId(payload.blobId, clusterMap));
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
                } catch (MessageFormatException e) {
                  e.printStackTrace();
                  throw new IllegalStateException();
                }
              }

              // get user metadata
              ids.clear();
              ids.add(new BlobId(payload.blobId, clusterMap));
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
                } catch (MessageFormatException e) {
                  e.printStackTrace();
                  throw new IllegalStateException();
                }
              }

              // get blob
              ids.clear();
              ids.add(new BlobId(payload.blobId, clusterMap));
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
                  int readsize = 0;
                  while (readsize < blobData.getSize()) {
                    readsize += blobData.getStream().read(blobout, readsize, (int) blobData.getSize() - readsize);
                  }
                  if (ByteBuffer.wrap(blobout).compareTo(ByteBuffer.wrap(payload.blob)) != 0) {
                    throw new IllegalStateException();
                  }
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
                  int readsize = 0;
                  while (readsize < blobAll.getBlobData().getSize()) {
                    readsize += blobAll.getBlobData()
                        .getStream()
                        .read(blobout, readsize, (int) blobAll.getBlobData().getSize() - readsize);
                  }
                  if (ByteBuffer.wrap(blobout).compareTo(ByteBuffer.wrap(payload.blob)) != 0) {
                    throw new IllegalStateException();
                  }
                } catch (MessageFormatException e) {
                  e.printStackTrace();
                  throw new IllegalStateException();
                }
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
}

