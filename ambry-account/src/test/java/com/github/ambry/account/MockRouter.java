/*
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.account;

import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.commons.ReadableStreamChannelInputStream;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.router.Callback;
import com.github.ambry.router.ChunkInfo;
import com.github.ambry.router.FutureResult;
import com.github.ambry.router.GetBlobOptions;
import com.github.ambry.router.GetBlobResult;
import com.github.ambry.router.PutBlobOptions;
import com.github.ambry.router.ReadableStreamChannel;
import com.github.ambry.router.Router;
import com.github.ambry.router.RouterErrorCode;
import com.github.ambry.router.RouterException;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO see if this can be removed/replaced with InMemoryRouter
public class MockRouter implements Router {
  private final static Logger logger = LoggerFactory.getLogger(MockRouter.class);
  private static final Random random = TestUtils.RANDOM;

  private final Lock lock = new ReentrantLock();
  private Map<String, BlobInfoAndData> allBlobs = new HashMap<>();

  private class BlobInfoAndData {
    private final BlobInfo info;
    private final byte[] data;

    BlobInfoAndData(BlobInfo info, byte[] data) {
      this.info = info;
      this.data = data;
    }

    BlobInfo getBlobInfo() {
      return info;
    }

    byte[] getData() {
      return data;
    }
  }

  @Override
  public Future<GetBlobResult> getBlob(String blobId, GetBlobOptions options, Callback<GetBlobResult> callback) {
    lock.lock();
    try {
      BlobInfoAndData blob = allBlobs.get(blobId);
      FutureResult<GetBlobResult> future = new FutureResult<>();
      if (blob == null) {
        Exception e = new RouterException("NotFound", RouterErrorCode.BlobDoesNotExist);
        future.done(null, e);
        if (callback != null) {
          callback.onCompletion(null, e);
        }
        return future;
      }
      // Discard the options and only return the BlobAll.
      ReadableStreamChannel channel = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(blob.getData()));
      GetBlobResult result = new GetBlobResult(blob.getBlobInfo(), channel);
      future.done(result, null);
      if (callback != null) {
        callback.onCompletion(result, null);
      }
      return future;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public Future<String> putBlob(BlobProperties blobProperties, byte[] userMetadata, ReadableStreamChannel channel,
      PutBlobOptions options, Callback<String> callback) {
    lock.lock();
    try {
      FutureResult<String> future = new FutureResult<>();
      long size = channel.getSize();
      try {
        InputStream input = new ReadableStreamChannelInputStream(channel);
        byte[] bytes = Utils.readBytesFromStream(input, (int) size);
        BlobInfoAndData blob = new BlobInfoAndData(new BlobInfo(blobProperties, userMetadata), bytes);
        String id;
        do {
          id = TestUtils.getRandomString(10);
        } while (allBlobs.putIfAbsent(id, blob) != null);
        future.done(id, null);
        if (callback != null) {
          callback.onCompletion(id, null);
        }
        return future;
      } catch (Exception e) {
        logger.error("Failed to put blob", e);
        future.done(null, e);
        if (callback != null) {
          callback.onCompletion(null, e);
        }
        return future;
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  public Future<String> stitchBlob(BlobProperties blobProperties, byte[] userMetadata, List<ChunkInfo> chunksToStitch,
      Callback<String> callback) {
    throw new UnsupportedOperationException("stichBlob is not supported by this mock");
  }

  @Override
  public Future<Void> deleteBlob(String blobId, String serviceId, Callback<Void> callback) {
    lock.lock();
    try {
      FutureResult<Void> future = new FutureResult<>();
      BlobInfoAndData blob = allBlobs.get(blobId);
      if (blob == null) {
        Exception e = new RouterException("NotFound", RouterErrorCode.BlobDoesNotExist);
        future.done(null, e);
        if (callback != null) {
          callback.onCompletion(null, e);
        }
        return future;
      }
      allBlobs.remove(blobId);
      future.done(null, null);
      if (callback != null) {
        callback.onCompletion(null, null);
      }
      return future;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public Future<Void> updateBlobTtl(String blobId, String serviceId, long expiresAtMs, Callback<Void> callback) {
    throw new UnsupportedOperationException("updateBlobTtl is not supported by this mock");
  }

  @Override
  public Future<Void> undeleteBlob(String blobId, String serviceId, Callback<Void> callback) {
    throw new UnsupportedOperationException("Undelete is currently unsupported");
  }

  /**
   * clear would remove all the blob in the map.
   */
  public void clear() {
    lock.lock();
    try {
      allBlobs.clear();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void close() throws IOException {
    // close will remove all the blobs
    clear();
  }
}
