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
package com.github.ambry.store;

import com.github.ambry.clustermap.ClusterAgentsFactory;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.Config;
import com.github.ambry.config.Default;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobStoreRecovery;
import com.github.ambry.messageformat.MessageFormatWriteSet;
import com.github.ambry.replication.FindToken;
import com.github.ambry.tools.util.ToolUtils;
import com.github.ambry.utils.ByteBufferChannel;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.utils.Utils.*;


/**
 * Copies messages from one {@link Store} to another and provides the ability to transform the message along the way.
 * Does not check whether the destination store fits all the messages from the source store - this check has to be
 * made before copying
 * <p/>
 * This tool requires the source store to *not* return blobs that have already been deleted when
 * {@link Store#findEntriesSince(FindToken, long)} is called. It is also expected to be run when both locations (src and
 * tgt) are offline.
 */
public class StoreCopier implements Closeable {

  /**
   * Config class for the {@link StoreCopier}.
   */
  private static class CopierConfig {

    /**
     * The path to the hardware layout file.
     */
    @Config("hardware.layout.file.path")
    final String hardwareLayoutFilePath;

    /**
     * The path to the partition layout file.
     */
    @Config("partition.layout.file.path")
    final String partitionLayoutFilePath;

    /**
     * The path of the directory where the source store files are.
     */
    @Config("src.store.dir")
    final String srcStoreDirPath;

    /**
     * The path of the directory where the target store files should be.
     */
    @Config("tgt.store.dir")
    final String tgtStoreDirPath;

    /**
     * The total capacity of the store.
     */
    @Config("store.capacity")
    final long storeCapacity;

    /**
     * The size of each fetch from the source store.
     */
    @Config("fetch.size.in.bytes")
    @Default("4 * 1024 * 1024")
    final long fetchSizeInBytes;

    CopierConfig(VerifiableProperties verifiableProperties) {
      hardwareLayoutFilePath = verifiableProperties.getString("hardware.layout.file.path");
      partitionLayoutFilePath = verifiableProperties.getString("partition.layout.file.path");
      srcStoreDirPath = verifiableProperties.getString("src.store.dir");
      tgtStoreDirPath = verifiableProperties.getString("tgt.store.dir");
      storeCapacity = verifiableProperties.getLong("store.capacity");
      fetchSizeInBytes = verifiableProperties.getLongInRange("fetch.size.in.bytes", 4 * 1024 * 1024, 1, Long.MAX_VALUE);
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(StoreCopier.class);
  private static final DecimalFormat df = new DecimalFormat(".###");

  private final String storeId;
  private final Store src;
  private final Store tgt;
  private final long fetchSizeInBytes;
  private final List<Transformer> transformers;
  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
  private boolean isOpen = true;

  public static void main(String[] args) throws Exception {
    VerifiableProperties properties = ToolUtils.getVerifiableProperties(args);
    CopierConfig config = new CopierConfig(properties);
    StoreConfig storeConfig = new StoreConfig(properties);
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(properties);
    ClusterAgentsFactory clusterAgentsFactory =
        Utils.getObj(clusterMapConfig.clusterMapClusterAgentsFactory, clusterMapConfig, config.hardwareLayoutFilePath,
            config.partitionLayoutFilePath);
    try (ClusterMap clusterMap = clusterAgentsFactory.getClusterMap()) {
      StoreKeyFactory storeKeyFactory = Utils.getObj(storeConfig.storeKeyFactory, clusterMap);
      File srcDir = new File(config.srcStoreDirPath);
      File tgtDir = new File(config.tgtStoreDirPath);
      StoreMetrics metrics = new StoreMetrics(clusterMap.getMetricRegistry());
      DiskSpaceAllocator diskSpaceAllocator =
          new DiskSpaceAllocator(false, null, 0, new StorageManagerMetrics(clusterMap.getMetricRegistry()));
      try (StoreCopier storeCopier = new StoreCopier("src", srcDir, tgtDir, config.storeCapacity,
          config.fetchSizeInBytes, storeConfig, metrics, storeKeyFactory, new DiskIOScheduler(null), diskSpaceAllocator,
          Collections.emptyList(), SystemTime.getInstance())) {
        storeCopier.copy(new StoreFindTokenFactory(storeKeyFactory).getNewFindToken());
      }
    }
  }

  /**
   * @param storeId the name/id of the {@link Store}.
   * @param srcDir the directory of the {@link Store} to be copied from
   * @param tgtDir the directory of the {@link Store} to be copied to.
   * @param storeCapacity the capacity of the store.
   * @param fetchSizeInBytes the size of each fetch from the soure store.
   * @param storeConfig {@link StoreConfig} that contains config to initiate a {@link BlobStore}.
   * @param metrics {@link StoreMetrics} to use for metrics.
   * @param storeKeyFactory the {@link StoreKeyFactory} to use for {@link StoreKey}s in the {@link Store}.
   * @param diskIOScheduler the {@link DiskIOScheduler} to use.
   * @param diskSpaceAllocator the {@link DiskSpaceAllocator} to use.
   * @param transformers the list of {@link Transformer} functions to execute. They will be executed in order.
   * @param time the {@link Time} instance to use.
   * @throws StoreException
   */
  public StoreCopier(String storeId, File srcDir, File tgtDir, long storeCapacity, long fetchSizeInBytes,
      StoreConfig storeConfig, StoreMetrics metrics, StoreKeyFactory storeKeyFactory, DiskIOScheduler diskIOScheduler,
      DiskSpaceAllocator diskSpaceAllocator, List<Transformer> transformers, Time time) throws StoreException {
    this.storeId = storeId;
    this.fetchSizeInBytes = fetchSizeInBytes;
    this.transformers = transformers;
    MessageStoreRecovery recovery = new BlobStoreRecovery();
    src = new BlobStore(storeId, storeConfig, null, null, diskIOScheduler, diskSpaceAllocator, metrics, metrics,
        srcDir.getAbsolutePath(), storeCapacity, storeKeyFactory, recovery, null, time);
    tgt = new BlobStore(storeId + "_tmp", storeConfig, scheduler, null, diskIOScheduler, diskSpaceAllocator, metrics,
        metrics, tgtDir.getAbsolutePath(), storeCapacity, storeKeyFactory, recovery, null, time);
    src.start();
    tgt.start();
  }

  @Override
  public void close() throws IOException {
    if (!isOpen) {
      return;
    }
    try {
      shutDownExecutorService(scheduler, 5, TimeUnit.MINUTES);
      src.shutdown();
      tgt.shutdown();
      isOpen = false;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * Copies data starting from {@code startToken} until all the data is copied.
   * @param startToken the {@link FindToken} to start copying from. It is expected that start token does not cause
   *                   the copier to attempt to copy blobs that have already been copied. If that happens, the boolean
   *                   in the return value will be {@code true}.
   * @return a {@link Pair} of the {@link FindToken} until which data has been copied and a {@link Boolean} indicating
   * whether the source had problems that were skipped over - like duplicates ({@code true} indicates that there were).
   * @throws Exception if there is any exception during processing
   */
  public Pair<FindToken, Boolean> copy(FindToken startToken) throws Exception {
    boolean sourceHasProblems = false;
    FindToken lastToken;
    FindToken token = startToken;
    do {
      lastToken = token;
      FindInfo findInfo = src.findEntriesSince(lastToken, fetchSizeInBytes);
      List<MessageInfo> messageInfos = findInfo.getMessageEntries();
      for (Transformer transformer : transformers) {
        transformer.warmup(messageInfos);
      }
      for (MessageInfo messageInfo : messageInfos) {
        logger.trace("Processing {} - isDeleted: {}, isExpired {}", messageInfo.getStoreKey(), messageInfo.isDeleted(),
            messageInfo.isExpired());
        if (!messageInfo.isExpired() && !messageInfo.isDeleted()) {
          if (tgt.findMissingKeys(Collections.singletonList(messageInfo.getStoreKey())).size() == 1) {
            StoreInfo storeInfo =
                src.get(Collections.singletonList(messageInfo.getStoreKey()), EnumSet.allOf(StoreGetOptions.class));
            MessageReadSet readSet = storeInfo.getMessageReadSet();
            if (readSet.sizeInBytes(0) > Integer.MAX_VALUE) {
              throw new IllegalStateException("Cannot copy blobs whose size > Integer.MAX_VALUE");
            }
            int size = (int) readSet.sizeInBytes(0);
            byte[] buf = new byte[size];
            readSet.writeTo(0, new ByteBufferChannel(ByteBuffer.wrap(buf)), 0, size);
            Message message = new Message(storeInfo.getMessageReadSetInfo().get(0), new ByteArrayInputStream(buf));
            for (Transformer transformer : transformers) {
              TransformationOutput tfmOutput = transformer.transform(message);
              if (tfmOutput.getException() != null) {
                throw tfmOutput.getException();
              } else {
                message = tfmOutput.getMsg();
              }
              if (message == null) {
                break;
              }
            }
            if (message == null) {
              logger.trace("Dropping {} because the transformers did not return a message", messageInfo.getStoreKey());
              continue;
            }
            MessageFormatWriteSet writeSet =
                new MessageFormatWriteSet(message.getStream(), Collections.singletonList(message.getMessageInfo()),
                    false);
            tgt.put(writeSet);
            MessageInfo tgtMsgInfo = message.getMessageInfo();
            if (tgtMsgInfo.isTtlUpdated()) {
              MessageInfo updateMsgInfo =
                  new MessageInfo(tgtMsgInfo.getStoreKey(), 0, false, true, tgtMsgInfo.getExpirationTimeInMs(),
                      tgtMsgInfo.getAccountId(), tgtMsgInfo.getContainerId(), tgtMsgInfo.getOperationTimeMs());
              tgt.updateTtl(Collections.singletonList(updateMsgInfo));
            }
            logger.trace("Copied {} as {}", messageInfo.getStoreKey(), tgtMsgInfo.getStoreKey());
          } else if (!messageInfo.isTtlUpdated()) {
            logger.warn("Found a duplicate entry for {} while copying data", messageInfo.getStoreKey());
            sourceHasProblems = true;
          }
        }
      }
      token = findInfo.getFindToken();
      double percentBytesRead = src.isEmpty() ? 100.0 : token.getBytesRead() * 100.0 / src.getSizeInBytes();
      logger.info("[{}] [{}] {}% copied", Thread.currentThread().getName(), storeId, df.format(percentBytesRead));
    } while (!token.equals(lastToken));
    return new Pair<>(token, sourceHasProblems);
  }
}
