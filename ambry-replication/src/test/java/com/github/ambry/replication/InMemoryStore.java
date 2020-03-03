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
package com.github.ambry.replication;

import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaState;
import com.github.ambry.messageformat.DeleteMessageFormatInputStream;
import com.github.ambry.messageformat.MessageFormatInputStream;
import com.github.ambry.messageformat.MessageFormatWriteSet;
import com.github.ambry.messageformat.TtlUpdateMessageFormatInputStream;
import com.github.ambry.messageformat.UndeleteMessageFormatInputStream;
import com.github.ambry.router.AsyncWritableChannel;
import com.github.ambry.router.Callback;
import com.github.ambry.store.FindInfo;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.MessageReadSet;
import com.github.ambry.store.MessageWriteSet;
import com.github.ambry.store.Store;
import com.github.ambry.store.StoreErrorCodes;
import com.github.ambry.store.StoreException;
import com.github.ambry.store.StoreGetOptions;
import com.github.ambry.store.StoreInfo;
import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreStats;
import com.github.ambry.store.Write;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.github.ambry.replication.ReplicationTest.*;


/**
 * A mock implementation of {@link Store} that store all details in memory.
 */
class InMemoryStore implements Store {
  ReplicaState currentState = ReplicaState.OFFLINE;

  class MockMessageReadSet implements MessageReadSet {

    // NOTE: all the functions in MockMessageReadSet are currently not used.

    private final List<ByteBuffer> buffers;
    private final List<StoreKey> storeKeys;

    MockMessageReadSet(List<ByteBuffer> buffers, List<StoreKey> storeKeys) {
      this.buffers = buffers;
      this.storeKeys = storeKeys;
    }

    @Override
    public long writeTo(int index, WritableByteChannel channel, long relativeOffset, long maxSize) throws IOException {
      ByteBuffer bufferToWrite = buffers.get(index);
      int savedPos = bufferToWrite.position();
      int savedLimit = bufferToWrite.limit();
      bufferToWrite.position((int) relativeOffset);
      bufferToWrite.limit((int) Math.min(maxSize + relativeOffset, bufferToWrite.capacity()));
      int sizeToWrite = bufferToWrite.remaining();
      while (bufferToWrite.hasRemaining()) {
        channel.write(bufferToWrite);
      }
      bufferToWrite.position(savedPos);
      bufferToWrite.limit(savedLimit);
      return sizeToWrite;
    }

    @Override
    public void writeTo(AsyncWritableChannel channel, Callback<Long> callback) {

    }

    @Override
    public int count() {
      return buffers.size();
    }

    @Override
    public long sizeInBytes(int index) {
      return buffers.get(index).limit();
    }

    @Override
    public StoreKey getKeyAt(int index) {
      return storeKeys.get(index);
    }

    @Override
    public void doPrefetch(int index, long relativeOffset, long size) {

    }
  }

  /**
   * Log that stores all data in memory.
   */
  class DummyLog implements Write {
    private final List<ByteBuffer> blobs;
    private long endOffSet;

    DummyLog(List<ByteBuffer> initialBlobs) {
      this.blobs = initialBlobs;
    }

    @Override
    public int appendFrom(ByteBuffer buffer) {
      ByteBuffer buf = ByteBuffer.allocate(buffer.remaining());
      buf.put(buffer);
      buf.flip();
      storeBuf(buf);
      return buf.capacity();
    }

    @Override
    public void appendFrom(ReadableByteChannel channel, long size) throws StoreException {
      ByteBuffer buf = ByteBuffer.allocate((int) size);
      int sizeRead = 0;
      try {
        while (sizeRead < size) {
          sizeRead += channel.read(buf);
        }
      } catch (IOException e) {
        StoreErrorCodes errorCode = StoreException.resolveErrorCode(e);
        throw new StoreException(errorCode.toString() + " while writing into dummy log", e, errorCode);
      }
      buf.flip();
      storeBuf(buf);
    }

    ByteBuffer getData(int index) {
      return blobs.get(index).duplicate();
    }

    long getEndOffSet() {
      return endOffSet;
    }

    private void storeBuf(ByteBuffer buffer) {
      blobs.add(buffer);
      endOffSet += buffer.capacity();
    }
  }

  private final ReplicationTest.StoreEventListener listener;
  private final DummyLog log;
  final List<MessageInfo> messageInfos;
  final PartitionId id;
  private boolean started;

  InMemoryStore(PartitionId id, List<MessageInfo> messageInfos, List<ByteBuffer> buffers,
      ReplicationTest.StoreEventListener listener) {
    if (messageInfos.size() != buffers.size()) {
      throw new IllegalArgumentException("message info size and buffer size does not match");
    }
    this.messageInfos = messageInfos;
    log = new DummyLog(buffers);
    this.listener = listener;
    this.id = id;
    started = true;
  }

  @Override
  public void start() throws StoreException {
    started = true;
  }

  @Override
  public StoreInfo get(List<? extends StoreKey> ids, EnumSet<StoreGetOptions> getOptions) throws StoreException {
    // unused function
    List<MessageInfo> infos = new ArrayList<>();
    List<ByteBuffer> buffers = new ArrayList<>();
    List<StoreKey> keys = new ArrayList<>();
    for (StoreKey id : ids) {
      for (int i = 0; i < messageInfos.size(); i++) {
        MessageInfo info = messageInfos.get(i);
        if (info.getStoreKey().equals(id)) {
          infos.add(info);
          buffers.add(log.getData(i));
          keys.add(info.getStoreKey());
        }
      }
    }
    return new StoreInfo(new MockMessageReadSet(buffers, keys), infos);
  }

  @Override
  public void put(MessageWriteSet messageSetToWrite) throws StoreException {
    List<MessageInfo> newInfos = messageSetToWrite.getMessageSetInfo();
    try {
      messageSetToWrite.writeTo(log);
    } catch (StoreException e) {
      throw new IllegalStateException(e);
    }
    List<MessageInfo> infos = new ArrayList<>();
    for (MessageInfo info : newInfos) {
      short lifeVersion = 0;
      if (info.getLifeVersion() != MessageInfo.LIFE_VERSION_FROM_FRONTEND) {
        lifeVersion = info.getLifeVersion();
      }
      if (info.isTtlUpdated()) {
        info = new MessageInfo(info.getStoreKey(), info.getSize(), info.isDeleted(), false, info.isUndeleted(),
            info.getExpirationTimeInMs(), info.getCrc(), info.getAccountId(), info.getContainerId(),
            info.getOperationTimeMs(), lifeVersion);
      }
      infos.add(info);
    }
    messageInfos.addAll(infos);
    if (listener != null) {
      listener.onPut(this, infos);
    }
  }

  @Override
  public void delete(List<MessageInfo> infos) throws StoreException {
    List<MessageInfo> infosToDelete = new ArrayList<>(infos.size());
    List<InputStream> inputStreams = new ArrayList();
    try {
      for (MessageInfo info : infos) {
        short lifeVersion = info.getLifeVersion();
        MessageInfo latestInfo = getMergedMessageInfo(info.getStoreKey(), messageInfos);
        if (latestInfo == null) {
          throw new StoreException("Cannot delete id " + info.getStoreKey() + " since it is not present in the index.",
              StoreErrorCodes.ID_Not_Found);
        }
        if (lifeVersion == MessageInfo.LIFE_VERSION_FROM_FRONTEND) {
          if (latestInfo.isDeleted()) {
            throw new StoreException(
                "Cannot delete id " + info.getStoreKey() + " since it is already deleted in the index.",
                StoreErrorCodes.ID_Deleted);
          }
          lifeVersion = latestInfo.getLifeVersion();
        } else {
          if ((latestInfo.isDeleted() && latestInfo.getLifeVersion() >= info.getLifeVersion()) || (
              latestInfo.getLifeVersion() > info.getLifeVersion())) {
            throw new StoreException(
                "Cannot delete id " + info.getStoreKey() + " since it is already deleted in the index.",
                StoreErrorCodes.Life_Version_Conflict);
          }
          lifeVersion = info.getLifeVersion();
        }
        MessageFormatInputStream stream =
            new DeleteMessageFormatInputStream(info.getStoreKey(), info.getAccountId(), info.getContainerId(),
                info.getOperationTimeMs(), lifeVersion);
        infosToDelete.add(new MessageInfo(info.getStoreKey(), stream.getSize(), true, info.isTtlUpdated(), false,
            info.getExpirationTimeInMs(), null, info.getAccountId(), info.getContainerId(), info.getOperationTimeMs(),
            lifeVersion));
        inputStreams.add(stream);
      }
      MessageFormatWriteSet writeSet =
          new MessageFormatWriteSet(new SequenceInputStream(Collections.enumeration(inputStreams)), infosToDelete,
              false);
      writeSet.writeTo(log);
      messageInfos.addAll(infosToDelete);
    } catch (Exception e) {
      throw (e instanceof StoreException ? (StoreException) e : new StoreException(e, StoreErrorCodes.Unknown_Error));
    }
  }

  @Override
  public void updateTtl(List<MessageInfo> infos) throws StoreException {
    List<MessageInfo> infosToUpdate = new ArrayList<>(infos.size());
    List<InputStream> inputStreams = new ArrayList<>();
    try {
      for (MessageInfo info : infos) {
        if (info.getExpirationTimeInMs() != Utils.Infinite_Time) {
          throw new StoreException("BlobStore only supports removing the expiration time",
              StoreErrorCodes.Update_Not_Allowed);
        }
        MessageInfo latestInfo = getMergedMessageInfo(info.getStoreKey(), messageInfos);
        if (latestInfo == null) {
          throw new StoreException("Cannot update TTL of " + info.getStoreKey() + " since it's not in the index",
              StoreErrorCodes.ID_Not_Found);
        } else if (latestInfo.isDeleted()) {
          throw new StoreException(
              "Cannot update TTL of " + info.getStoreKey() + " since it is already deleted in the index.",
              StoreErrorCodes.ID_Deleted);
        } else if (latestInfo.isTtlUpdated()) {
          throw new StoreException("TTL of " + info.getStoreKey() + " is already updated in the index.",
              StoreErrorCodes.Already_Updated);
        }
        short lifeVersion = latestInfo.getLifeVersion();
        MessageFormatInputStream stream =
            new TtlUpdateMessageFormatInputStream(info.getStoreKey(), info.getAccountId(), info.getContainerId(),
                info.getExpirationTimeInMs(), info.getOperationTimeMs(), lifeVersion);
        infosToUpdate.add(
            new MessageInfo(info.getStoreKey(), stream.getSize(), false, true, false, info.getExpirationTimeInMs(),
                null, info.getAccountId(), info.getContainerId(), info.getOperationTimeMs(), lifeVersion));
        inputStreams.add(stream);
      }
      MessageFormatWriteSet writeSet =
          new MessageFormatWriteSet(new SequenceInputStream(Collections.enumeration(inputStreams)), infosToUpdate,
              false);
      writeSet.writeTo(log);
      messageInfos.addAll(infosToUpdate);
    } catch (Exception e) {
      throw (e instanceof StoreException ? (StoreException) e : new StoreException(e, StoreErrorCodes.Unknown_Error));
    }
  }

  @Override
  public short undelete(MessageInfo info) throws StoreException {
    StoreKey key = info.getStoreKey();
    MessageInfo deleteInfo = getMessageInfo(key, messageInfos, true, false, false);
    if (info.getLifeVersion() == -1 && deleteInfo == null) {
      throw new StoreException("Key " + key + " not delete yet", StoreErrorCodes.ID_Not_Deleted);
    }
    short lifeVersion = info.getLifeVersion();
    MessageInfo latestInfo = deleteInfo;
    if (info.getLifeVersion() == MessageInfo.LIFE_VERSION_FROM_FRONTEND) {
      if (deleteInfo == null) {
        throw new StoreException(
            "Id " + key + " requires first value to be a put and last value to be a delete",
            StoreErrorCodes.ID_Not_Deleted);
      }
      lifeVersion = (short) (deleteInfo.getLifeVersion() + 1);
    } else {
      if (deleteInfo == null) {
        latestInfo = getMergedMessageInfo(key, messageInfos);
      }
    }
    try {
      MessageFormatInputStream stream =
          new UndeleteMessageFormatInputStream(key, info.getAccountId(), info.getContainerId(),
              info.getOperationTimeMs(), lifeVersion);
      // Update info to add stream size;
      info = new MessageInfo(key, stream.getSize(), false, latestInfo.isTtlUpdated(), true,
          latestInfo.getExpirationTimeInMs(), null, info.getAccountId(), info.getContainerId(),
          info.getOperationTimeMs(), lifeVersion);
      MessageFormatWriteSet writeSet = new MessageFormatWriteSet(stream, Collections.singletonList(info), false);
      writeSet.writeTo(log);
      messageInfos.add(info);
      return lifeVersion;
    } catch (Exception e) {
      throw new StoreException("Unknown error while trying to undelete blobs from store", e,
          StoreErrorCodes.Unknown_Error);
    }
  }

  @Override
  public FindInfo findEntriesSince(FindToken token, long maxSizeOfEntries) throws StoreException {
    // unused function
    MockFindToken mockToken = (MockFindToken) token;
    List<MessageInfo> entriesToReturn = new ArrayList<>();
    long currentSizeOfEntriesInBytes = 0;
    int index = mockToken.getIndex();
    Set<StoreKey> processedKeys = new HashSet<>();
    while (currentSizeOfEntriesInBytes < maxSizeOfEntries && index < messageInfos.size()) {
      StoreKey key = messageInfos.get(index).getStoreKey();
      if (processedKeys.add(key)) {
        entriesToReturn.add(getMergedMessageInfo(key, messageInfos));
      }
      // still use the size of the put (if the original picked up is the put.
      currentSizeOfEntriesInBytes += messageInfos.get(index).getSize();
      index++;
    }

    int startIndex = mockToken.getIndex();
    int totalSizeRead = 0;
    for (int i = 0; i < startIndex; i++) {
      totalSizeRead += messageInfos.get(i).getSize();
    }
    totalSizeRead += currentSizeOfEntriesInBytes;
    return new FindInfo(entriesToReturn,
        new MockFindToken(mockToken.getIndex() + entriesToReturn.size(), totalSizeRead));
  }

  @Override
  public Set<StoreKey> findMissingKeys(List<StoreKey> keys) throws StoreException {
    Set<StoreKey> keysMissing = new HashSet<>();
    for (StoreKey key : keys) {
      boolean found = false;
      for (MessageInfo messageInfo : messageInfos) {
        if (messageInfo.getStoreKey().equals(key)) {
          found = true;
          break;
        }
      }
      if (!found) {
        keysMissing.add(key);
      }
    }
    return keysMissing;
  }

  @Override
  public MessageInfo findKey(StoreKey key) throws StoreException {
    return getMergedMessageInfo(key, messageInfos);
  }

  @Override
  public StoreStats getStoreStats() {
    return null;
  }

  @Override
  public boolean isKeyDeleted(StoreKey key) throws StoreException {
    return getMessageInfo(key, messageInfos, true, false, false) != null;
  }

  @Override
  public long getSizeInBytes() {
    return log.getEndOffSet();
  }

  @Override
  public boolean isEmpty() {
    return log.blobs.isEmpty();
  }

  @Override
  public boolean isBootstrapInProgress() {
    throw new UnsupportedOperationException("Method not supported");
  }

  @Override
  public boolean isDecommissionInProgress() {
    throw new UnsupportedOperationException("Method not supported");
  }

  @Override
  public void completeBootstrap() {
    // no-op
  }

  @Override
  public void setCurrentState(ReplicaState state) {
    currentState = state;
  }

  @Override
  public ReplicaState getCurrentState() {
    return currentState;
  }

  @Override
  public long getEndPositionOfLastPut() throws StoreException {
    throw new UnsupportedOperationException("Method not supported");
  }

  @Override
  public boolean recoverFromDecommission() {
    return false;
  }

  @Override
  public void shutdown() throws StoreException {
    started = false;
  }

  @Override
  public boolean isStarted() {
    return started;
  }
}
