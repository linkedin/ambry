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
package com.github.ambry.store;

import com.codahale.metrics.Timer;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.replication.FindToken;
import com.github.ambry.replication.FindTokenType;
import com.github.ambry.utils.CrcInputStream;
import com.github.ambry.utils.CrcOutputStream;
import com.github.ambry.utils.Time;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class that provides functionality to perform hard deletes.
 */
public class HardDeleter implements Runnable {

  public static final short Cleanup_Token_Version_V0 = 0;
  public static final short Cleanup_Token_Version_V1 = 1;
  private static final String Cleanup_Token_Filename = "cleanuptoken";
  //how long to sleep if token does not advance.
  static final long HARD_DELETE_SLEEP_TIME_ON_CAUGHT_UP_MS = 60 * Time.MsPerSec;

  final AtomicBoolean enabled = new AtomicBoolean(true);
  private volatile boolean awaitingAfterCaughtUp = false;

  // The method to call before hard deleter thread performs hard delete if this is not null.
  // This is only for test.
  private volatile Supplier<Void> injectedBeforeHardDeleteSupplier;
  private final StoreConfig config;
  private final StoreMetrics metrics;
  private final String dataDir;
  private final Log log;
  private final PersistentIndex index;
  private final MessageStoreHardDelete hardDelete;
  private final StoreKeyFactory factory;
  private final Time time;
  private final DiskIOScheduler diskIOScheduler;
  private final int scanSizeInBytes;
  private final int messageRetentionSeconds;
  private final CountDownLatch shutdownLatch = new CountDownLatch(1);
  private final Logger logger = LoggerFactory.getLogger(getClass());

  /* A range of entries is maintained during the hard delete operation. All the entries corresponding to an ongoing
   * hard delete will be from this range. The reason to keep this range is to finish off any incomplete and ongoing
   * hard deletes when we do a crash recovery.
   * Following tokens are maintained:
   * startTokenSafeToPersist <= startTokenBeforeLogFlush <= startToken <= endToken
   *
   * Ongoing hard deletes are for entries within startToken and endToken. These keep getting incremented as and when
   * hard deletes happen. The cleanup token that is persisted periodically is used during recovery to figure out the
   * range on which recovery is to be done. The end token to persist is the endToken that we maintain. However, the
   * start token that is persisted has to be a token up to which the hard deletes that were performed have been
   * flushed. Since the index persistor runs asynchronously to the hard delete thread, a few other tokens are used to
   * help safely persist tokens:
   *
   * startTokenSafeToPersist:  This will always be a value up to which the log has been flushed, and is a token safe
   *                           to be persisted in the cleanupToken file. The 'current' start token can be greater than
   *                           this value.
   * startTokenBeforeLogFlush: This token is set to the current start token just before log flush and once the log is
   *                           flushed, this is used to set startTokenSafeToPersist.
   */
  private volatile FindToken startTokenBeforeLogFlush;
  private volatile FindToken startTokenSafeToPersist;
  private volatile FindToken startToken;
  private volatile FindToken endToken;
  private StoreFindToken recoveryEndToken;
  // used to protect hardDeleteRecoveryRange and persistCleanupToken
  private final ReentrantLock persistFileLock = new ReentrantLock();
  private HardDeletePersistInfo hardDeleteRecoveryRange = new HardDeletePersistInfo();
  private boolean isCaughtUp = false;
  private final AtomicBoolean paused = new AtomicBoolean(false);
  private final ReentrantLock hardDeleteLock = new ReentrantLock();
  private final Condition pauseCondition = hardDeleteLock.newCondition();
  static final String HARD_DELETE_CLEANUP_JOB_NAME = "hard_deleter_cleanup";

  HardDeleter(StoreConfig config, StoreMetrics metrics, String dataDir, Log log, PersistentIndex index,
      MessageStoreHardDelete hardDelete, StoreKeyFactory factory, DiskIOScheduler diskIOScheduler, Time time) {
    this.config = config;
    this.metrics = metrics;
    this.dataDir = dataDir;
    this.log = log;
    this.index = index;
    this.hardDelete = hardDelete;
    this.factory = factory;
    this.diskIOScheduler = diskIOScheduler;
    this.time = time;
    // Times 10 is an optimization for findDeletedEntriesSince, which keeps delete entries only in the end.
    scanSizeInBytes = config.storeHardDeleteOperationsBytesPerSec * 10;
    messageRetentionSeconds = (int) TimeUnit.DAYS.toSeconds(config.storeDeletedMessageRetentionDays);
  }

  @Override
  public void run() {
    try {
      logger.trace("Starting hard delete runnable for {}", dataDir);
      while (enabled.get()) {
        hardDeleteLock.lock();
        try {
          while (enabled.get() && isPaused()) {
            logger.info("Hard delete entering pause state for {}", dataDir);
            pauseCondition.await();
          }
          if (enabled.get()) {
            if (!hardDelete()) {
              isCaughtUp = true;
              logger.trace("Waiting for {} ms after caught up for {}", HARD_DELETE_SLEEP_TIME_ON_CAUGHT_UP_MS, dataDir);
              awaitingAfterCaughtUp = true;
              time.await(pauseCondition, HARD_DELETE_SLEEP_TIME_ON_CAUGHT_UP_MS);
              awaitingAfterCaughtUp = false;
            } else if (isCaughtUp) {
              isCaughtUp = false;
              logger.info("Resumed hard deletes for {} after having caught up", dataDir);
            }
          }
        } catch (StoreException e) {
          if (e.getErrorCode() != StoreErrorCodes.Store_Shutting_Down) {
            logger.error("Caught store exception: ", e);
          } else {
            logger.trace("Caught exception during hard deletes", e);
          }
        } catch (InterruptedException e) {
          logger.trace("Caught exception during hard deletes", e);
        } finally {
          hardDeleteLock.unlock();
        }
      }
    } finally {
      close();
    }
  }

  /**
   * Pauses hard deletes. This guarantees that any on-going hard deletes have been completed, flushed and persisted and
   * the hard delete tokens are persisted before returning from pause. Hard delete will be in a paused state until
   * {@link #resume()} is called.
   */
  void pause() throws StoreException, IOException {
    hardDeleteLock.lock();
    try {
      if (paused.compareAndSet(false, true)) {
        logger.info("HardDelete thread has been paused ");
        index.persistIndex();
        persistFileLock.lock();
        try {
          // PersistCleanupToken because paused changed.
          pruneHardDeleteRecoveryRange();
          persistCleanupToken();
        } finally {
          persistFileLock.unlock();
        }
      }
    } finally {
      hardDeleteLock.unlock();
    }
  }

  /**
   * Resumes hard deletes, if {@link #pause()} has been called prior to this.
   */
  void resume() {
    hardDeleteLock.lock();
    try {
      if (paused.compareAndSet(true, false)) {
        startToken =
            startTokenBeforeLogFlush = startTokenSafeToPersist = endToken = index.revalidateFindToken(startToken);
        pauseCondition.signal();
      }
      logger.info("Hard delete resumed for {}", dataDir);
    } finally {
      hardDeleteLock.unlock();
    }
  }

  /**
   * Does the recovery of hard deleted blobs (means redoing the hard deletes)
   *
   * This method first populates hardDeleteRecoveryRange with the blob information read from the cleanup
   * token file. It then passes the messageStoreRecoveryInfo to the MessageStoreHardDelete component so that the latter
   * can use it for recovering the information that may be required to hard delete the messages that are being hard
   * deleted as part of recovery.
   *
   * @throws StoreException on version mismatch.
   */
  void performRecovery() throws StoreException {
    try {
      readCleanupTokenAndPopulateRecoveryRange();
      if (hardDeleteRecoveryRange.getSize() == 0) {
        return;
      }

      /* First create the readOptionsList */
      List<BlobReadOptions> readOptionsList = new ArrayList<>();
      List<byte[]> messageStoreRecoveryInfoList = new ArrayList<>();
      for (HardDeletePersistItem item : hardDeleteRecoveryRange.getHardDeletePersistItemList()) {
        readOptionsList.add(item.blobReadOptions);
        messageStoreRecoveryInfoList.add(item.messagesStoreRecoveryInfo);
      }
      hardDeleteRecoveryRange.clear();

        /* Next, perform the log write. The token file does not have to be persisted again as only entries that are
           currently in it are being hard deleted as part of recovery. */
      StoreMessageReadSet readSet = new StoreMessageReadSet(readOptionsList);
      Iterator<HardDeleteInfo> hardDeleteIterator =
          hardDelete.getHardDeleteMessages(readSet, factory, messageStoreRecoveryInfoList);

      Iterator<BlobReadOptions> readOptionsIterator = readOptionsList.iterator();
      while (hardDeleteIterator.hasNext()) {
        if (!enabled.get()) {
          throw new StoreException("Aborting hard deletes as store is shutting down",
              StoreErrorCodes.Store_Shutting_Down);
        }
        HardDeleteInfo hardDeleteInfo = hardDeleteIterator.next();
        BlobReadOptions readOptions = readOptionsIterator.next();
        if (hardDeleteInfo == null) {
          metrics.hardDeleteFailedCount.inc(1);
        } else {
          LogSegment logSegment = log.getSegment(readOptions.getLogSegmentName());
          logSegment.writeFrom(hardDeleteInfo.getHardDeleteChannel(),
              readOptions.getOffset() + hardDeleteInfo.getStartOffsetInMessage(),
              hardDeleteInfo.getHardDeletedMessageSize());
          metrics.hardDeleteDoneCount.inc(1);
        }
      }
    } catch (IOException e) {
      metrics.hardDeleteExceptionsCount.inc();
      StoreErrorCodes errorCode = StoreException.resolveErrorCode(e);
      throw new StoreException(errorCode.toString() + " occurred while performing hard delete ", e, errorCode);
    }
      /* Now that all the blobs in the range were successfully hard deleted, the next time hard deletes can be resumed
         at recoveryEndToken. */
    startToken = endToken = recoveryEndToken;
  }

  /**
   * Prunes the hardDeleteRecoveryRange so that all the information for keys before startTokenSafeToPersist are
   * removed. This is safe as the hard deleted writes of those keys are guaranteed to have been flushed in the log.
   * All the entries from startTokenSafeToPersist to endToken must be maintained in hardDeleteRecoveryRange.
   * hardDeleteRecoveryRange may still have information of keys that have been persisted in certain cases, but this
   * does not affect the safety.
   * Note that we don't need any synchronization for this method as the hardDeleteRecoveryRange and all other
   * variables except startTokenSafeToPersist are only modified by the hardDeleteThread. Since startTokenSafeToPersist
   * gets modified by the IndexPersistorThread while this operation is in progress, we save it off first and use the
   * saved off value subsequently.
   */
  void pruneHardDeleteRecoveryRange() {
    StoreFindToken logFlushedTillToken = (StoreFindToken) startTokenSafeToPersist;
    if (logFlushedTillToken != null && !logFlushedTillToken.getType().equals(FindTokenType.Uninitialized)) {
      if (logFlushedTillToken.equals(endToken)) {
        hardDeleteRecoveryRange.clear();
      } else if (logFlushedTillToken.getStoreKey() != null) {
          /* Avoid pruning if the token is journal based as it is complicated and unnecessary. This is because, in the
             recovery range, keys are stored not offsets. It should be okay to not prune though, as there is no
             correctness issue. Additionally, since this token is journal based, it is highly likely that this token
             will soon become equal to endtoken, in which case we will prune everything (in the "if case" above).
             If the token is index based, safely prune off entries that have already been flushed in the log */
        hardDeleteRecoveryRange.pruneTill(logFlushedTillToken);
      }
    }
    if (hardDeleteRecoveryRange.getSize() > 0) {
      logger.trace("Pruned Hard delete recovery range : {} to {} with size {} for {}",
          hardDeleteRecoveryRange.items.get(0).blobReadOptions,
          hardDeleteRecoveryRange.items.get(hardDeleteRecoveryRange.getSize() - 1).blobReadOptions,
          hardDeleteRecoveryRange.getSize(), dataDir);
    } else {
      logger.trace("Hard delete recovery range is empty for {}", dataDir);
    }
  }

  HardDeletePersistInfo getHardDeleteRecoveryRange() {
    return hardDeleteRecoveryRange;
  }

  /**
   * Finds deleted entries from the index, persists tokens and calls performHardDelete to delete the corresponding put
   * records in the log.
   * Note: At this time, expired blobs are not hard deleted.
   *
   * Sp is the token till which log is persisted and is therefore safe to be used as the start point during recovery.
   * (S, E] represents the range of elements being hard deleted at any given time.
   *
   * The algorithm is as follows:
   * 1. Start at the current token S.
   * 2. {E, entries} = findDeletedEntriesSince(S).
   * 3. Persist {Sp, E} and at least all the entries in the range (Sp, E] to help with recovery if we crash.
   * 4. perform hard deletes of entries in this range.
   * 5. set S = E
   * 6. Index Persistor runs in the background and
   *    a) saves Sf = S
   *    b) flushes log (so everything up to Sf is surely flushed in the log)
   *    c) sets Sp = Sf
   *
   * The guarantee provided is that for any persisted token pair (Sp, E):
   *    - all the hard deletes till point Sp have been flushed in the log; and
   *    - all ongoing hard deletes and unflushed hard deletes are somewhere between Sp and E, so during recovery this
   *    is the range to be recovered.
   *
   * @return true if the token moved forward, false otherwise.
   */
  boolean hardDelete() throws StoreException {
    if (index.getCurrentEndOffset().compareTo(index.getStartOffset()) > 0) {
      final Timer.Context context = metrics.hardDeleteTime.time();
      try {
        FindInfo info =
            index.findDeletedEntriesSince(startToken, scanSizeInBytes, time.seconds() - messageRetentionSeconds);
        endToken = info.getFindToken();
        logger.trace("New range for hard deletes : startToken {}, endToken {} for {}", startToken, info.getFindToken(),
            dataDir);
        if (!endToken.equals(startToken)) {
          if (!info.getMessageEntries().isEmpty()) {
            if (injectedBeforeHardDeleteSupplier != null) {
              injectedBeforeHardDeleteSupplier.get();
            }
            performHardDeletes(info.getMessageEntries());
          }
          startToken = endToken;
          return true;
        }
      } catch (StoreException e) {
        if (e.getErrorCode() != StoreErrorCodes.Store_Shutting_Down) {
          metrics.hardDeleteExceptionsCount.inc();
        }
        throw e;
      } finally {
        context.stop();
      }
    }
    return false;
  }

  /**
   * This method will be called before the log is flushed.
   * In production, this will be called in the PersistentIndex's IndexPersistor thread.
   */
  void preLogFlush() {
    /* Save the current start token before the log gets flushed */
    startTokenBeforeLogFlush = startToken;
  }

  /**
   * This method will be called after the log is flushed.
   * In production, this will be called in the PersistentIndex's IndexPersistor thread.
   */
  void postLogFlush() {
    if (enabled.get() && !isPaused()) {
      /* start token saved before the flush is now safe to be persisted */
      persistFileLock.lock();
      startTokenSafeToPersist = startTokenBeforeLogFlush;
      try {
        // PersistCleanupToken because startTokenSafeToPersist changed.
        pruneHardDeleteRecoveryRange();
        persistCleanupToken();
      } catch (Exception e) {
        logger.error("Failed to persist cleanup token", e);
      } finally {
        persistFileLock.unlock();
      }
    }
  }

  /**
   * @return the {@link #startTokenSafeToPersist}
   */
  FindToken getStartTokenSafeToPersistTo() {
    return startTokenSafeToPersist;
  }

  /**
   * @return the {@link #startToken}
   */
  FindToken getStartToken() {
    return startToken;
  }

  /**
   * @return the {@link #endToken}
   */
  FindToken getEndToken() {
    return endToken;
  }

  /**
   * Set the {@link #injectedBeforeHardDeleteSupplier}.
   * @param supplier
   */
  void setInjectedBeforeHardDeleteSupplier(Supplier<Void> supplier) {
    this.injectedBeforeHardDeleteSupplier = supplier;
  }

  /**
   * Gets the number of bytes processed so far
   * @return the number of bytes processed so far as represented by the start token. Note that if the token is
   * index based, this is at segment granularity.
   */
  long getProgress() {
    StoreFindToken token = (StoreFindToken) startToken;
    return token.getType().equals(FindTokenType.Uninitialized) ? 0
        : index.getAbsolutePositionInLogForOffset(token.getOffset());
  }

  /**
   * Returns true if the hard delete is currently paused.
   * @return true if paused, false otherwise.
   */
  boolean isPaused() {
    return paused.get();
  }

  /**
   * Returns true if the hard delete is currently enabled and not paused
   * @return true if enabled and not paused, false otherwise.
   */
  boolean isRunning() {
    return shutdownLatch.getCount() != 0 && !isPaused();
  }

  /**
   * Returns true if the hard delete thread has caught up, that is if the token did not advance in the last iteration
   * @return true if caught up, false otherwise.
   */
  boolean isCaughtUp() {
    return isCaughtUp;
  }

  void shutdown() throws InterruptedException {
    long startTimeInMs = time.milliseconds();
    try {
      if (enabled.get()) {
        logger.info("Hard delete shutdown initiated for store {} ", dataDir);
        enabled.set(false);
        hardDeleteLock.lock();
        if (awaitingAfterCaughtUp) {
          logger.info("HardDelete is awaiting after caught up for store {} ", dataDir);
        } else {
          logger.info("HardDelete in progress for store {} ", dataDir);
        }
        try {
          pauseCondition.signal();
        } finally {
          hardDeleteLock.unlock();
        }
        shutdownLatch.await();
        logger.info("Hard delete shutdown complete for store {} ", dataDir);
      }
    } finally {
      metrics.hardDeleteShutdownTimeInMs.update(time.milliseconds() - startTimeInMs);
    }
  }

  void close() {
    enabled.set(false);
    shutdownLatch.countDown();
  }

  /**
   * Reads from the cleanupToken file and adds into hardDeleteRecoveryRange the info for all the messages persisted
   * in the file. If cleanupToken is non-existent or if there is a crc failure, resets the token.
   * This method calls into MessageStoreHardDelete interface to let it read the persisted recovery metadata from the
   * stream.
   * @throws StoreException on version mismatch.
   */
  private void readCleanupTokenAndPopulateRecoveryRange() throws IOException, StoreException {
    File cleanupTokenFile = new File(dataDir, Cleanup_Token_Filename);
    StoreFindToken recoveryStartToken = recoveryEndToken = new StoreFindToken();
    startToken = startTokenBeforeLogFlush = startTokenSafeToPersist = endToken = new StoreFindToken();
    if (cleanupTokenFile.exists()) {
      CrcInputStream crcStream = new CrcInputStream(new FileInputStream(cleanupTokenFile));
      DataInputStream stream = new DataInputStream(crcStream);
      try {
        short version = stream.readShort();
        switch (version) {
          case Cleanup_Token_Version_V0:
            recoveryStartToken = StoreFindToken.fromBytes(stream, factory);
            recoveryEndToken = StoreFindToken.fromBytes(stream, factory);
            hardDeleteRecoveryRange = new HardDeletePersistInfo(stream, factory);
            break;
          case Cleanup_Token_Version_V1:
            recoveryStartToken = StoreFindToken.fromBytes(stream, factory);
            recoveryEndToken = StoreFindToken.fromBytes(stream, factory);
            paused.set(stream.readByte() == (byte) 1);
            hardDeleteRecoveryRange = new HardDeletePersistInfo(stream, factory);
            break;
          default:
            hardDeleteRecoveryRange.clear();
            metrics.hardDeleteIncompleteRecoveryCount.inc();
            throw new StoreException("Invalid version in cleanup token " + dataDir,
                StoreErrorCodes.Index_Version_Error);
        }
        long crc = crcStream.getValue();
        if (crc != stream.readLong()) {
          hardDeleteRecoveryRange.clear();
          metrics.hardDeleteIncompleteRecoveryCount.inc();
          throw new StoreException(
              "Crc check does not match for cleanup token file for dataDir " + dataDir + " aborting. ",
              StoreErrorCodes.Illegal_Index_State);
        }
        if (config.storeSetFilePermissionEnabled) {
          Files.setPosixFilePermissions(cleanupTokenFile.toPath(), config.storeDataFilePermission);
        }
      } catch (IOException e) {
        hardDeleteRecoveryRange.clear();
        metrics.hardDeleteIncompleteRecoveryCount.inc();
        throw new StoreException("Failed to read cleanup token ", e, StoreErrorCodes.Initialization_Error);
      } finally {
        stream.close();
      }
    }
      /* If all the information was successfully read and there are no crc check failures, then the next time hard
         deletes are done, it can start at least at the recoveryStartToken.
       */
    startToken = startTokenBeforeLogFlush = startTokenSafeToPersist = endToken = recoveryStartToken;
  }

  private void persistCleanupToken() throws IOException, StoreException {
        /* The cleanup token format is as follows:
           --
           token_version

           Cleanup_Token_Version_V0
            startTokenForRecovery
            endTokenForRecovery
            numBlobsInRange
            --
            blob1_blobReadOptions {version, offset, sz, ttl, key}
            blob2_blobReadOptions
            ....
            blobN_blobReadOptions
            --
            length_of_blob1_messageStoreRecoveryInfo
            blob1_messageStoreRecoveryInfo {headerVersion, userMetadataVersion, userMetadataSize, blobRecordVersion, blobStreamSize}
            length_of_blob2_messageStoreRecoveryInfo
            blob2_messageStoreRecoveryInfo
            ....
            length_of_blobN_messageStoreRecoveryInfo
            blobN_messageStoreRecoveryInfo

           Cleanup_Token_Version_V1
            startTokenForRecovery
            endTokenForRecovery
            pause flag
            numBlobsInRange
            --
            blob1_blobReadOptions {version, offset, sz, ttl, key}
            blob2_blobReadOptions
            ....
            blobN_blobReadOptions
            --
            length_of_blob1_messageStoreRecoveryInfo
            blob1_messageStoreRecoveryInfo {headerVersion, userMetadataVersion, userMetadataSize, blobRecordVersion, blobStreamSize}
            length_of_blob2_messageStoreRecoveryInfo
            blob2_messageStoreRecoveryInfo
            ....
            length_of_blobN_messageStoreRecoveryInfo
            blobN_messageStoreRecoveryInfo

           --
           crc
           ---
         */
    if (endToken == null) {
      return;
    }
    final Timer.Context context = metrics.cleanupTokenFlushTime.time();
    File tempFile = new File(dataDir, Cleanup_Token_Filename + ".tmp");
    File actual = new File(dataDir, Cleanup_Token_Filename);
    FileOutputStream fileStream = new FileOutputStream(tempFile);
    CrcOutputStream crc = new CrcOutputStream(fileStream);
    DataOutputStream writer = new DataOutputStream(crc);
    try {
      // write the current version
      writer.writeShort(Cleanup_Token_Version_V1);
      writer.write(startTokenSafeToPersist.toBytes());
      writer.write(endToken.toBytes());
      writer.writeByte(isPaused() ? (byte) 1 : (byte) 0);
      writer.write(hardDeleteRecoveryRange.toBytes());
      long crcValue = crc.getValue();
      writer.writeLong(crcValue);
      fileStream.getChannel().force(true);
      tempFile.renameTo(actual);
      if (config.storeSetFilePermissionEnabled) {
        Files.setPosixFilePermissions(actual.toPath(), config.storeDataFilePermission);
      }
    } catch (IOException e) {
      StoreErrorCodes errorCode = StoreException.resolveErrorCode(e);
      throw new StoreException(
          errorCode.toString() + " while persisting cleanup tokens to disk " + tempFile.getAbsoluteFile(), errorCode);
    } finally {
      writer.close();
      context.stop();
    }
    logger.debug("Completed writing cleanup tokens to file {}", actual.getAbsolutePath());
  }

  /**
   * Performs hard deletes of all the messages in the messageInfoList.
   * Gets a view of the records in the log for those messages and calls cleanup to get the appropriate replacement
   * records, and then replaces the records in the log with the corresponding replacement records.
   * @param messageInfoList: The messages to be hard deleted in the log.
   */
  private void performHardDeletes(List<MessageInfo> messageInfoList) throws StoreException {
    try {
      EnumSet<StoreGetOptions> getOptions = EnumSet.of(StoreGetOptions.Store_Include_Deleted);
      List<BlobReadOptions> readOptionsList = new ArrayList<BlobReadOptions>(messageInfoList.size());

      /* First create the readOptionsList */
      for (MessageInfo info : messageInfoList) {
        if (!enabled.get()) {
          throw new StoreException("Aborting, store is shutting down", StoreErrorCodes.Store_Shutting_Down);
        }
        try {
          BlobReadOptions readInfo = index.getBlobReadInfo(info.getStoreKey(), getOptions);
          readOptionsList.add(readInfo);
        } catch (StoreException e) {
          logger.debug("Failed to read blob info for blobid {} during hard deletes, ignoring. Caught exception {}",
              info.getStoreKey(), e);
          metrics.hardDeleteExceptionsCount.inc();
        }
      }

      List<LogWriteInfo> logWriteInfoList = new ArrayList<LogWriteInfo>();

      StoreMessageReadSet readSet = new StoreMessageReadSet(readOptionsList);
      Iterator<HardDeleteInfo> hardDeleteIterator = hardDelete.getHardDeleteMessages(readSet, factory, null);
      Iterator<BlobReadOptions> readOptionsIterator = readOptionsList.iterator();

      persistFileLock.lock();
      try {
        /* Next, get the information to persist hard delete recovery info. Get all the information and save it, as only
         * after the whole range is persisted can we start with the actual log write */
        while (hardDeleteIterator.hasNext()) {
          if (!enabled.get()) {
            throw new StoreException("Aborting hard deletes as store is shutting down",
                StoreErrorCodes.Store_Shutting_Down);
          }
          HardDeleteInfo hardDeleteInfo = hardDeleteIterator.next();
          BlobReadOptions readOptions = readOptionsIterator.next();
          if (hardDeleteInfo == null) {
            metrics.hardDeleteFailedCount.inc(1);
          } else {
            hardDeleteRecoveryRange.addMessageInfo(readOptions, hardDeleteInfo.getRecoveryInfo(),
                (StoreFindToken) startToken);
            LogSegment logSegment = log.getSegment(readOptions.getLogSegmentName());
            logWriteInfoList.add(new LogWriteInfo(logSegment, hardDeleteInfo.getHardDeleteChannel(),
                readOptions.getOffset() + hardDeleteInfo.getStartOffsetInMessage(),
                hardDeleteInfo.getHardDeletedMessageSize()));
          }
        }

        if (readOptionsIterator.hasNext()) {
          metrics.hardDeleteExceptionsCount.inc(1);
          throw new IllegalStateException("More number of blobReadOptions than hardDeleteMessages");
        }

        // PersistCleanupToken because EndToken and the RecoveryRange changed.
        persistCleanupToken();
      } finally {
        persistFileLock.unlock();
      }

      /* Finally, write the hard delete stream into the Log */
      for (LogWriteInfo logWriteInfo : logWriteInfoList) {
        if (!enabled.get()) {
          throw new StoreException("Aborting hard deletes as store is shutting down",
              StoreErrorCodes.Store_Shutting_Down);
        }
        diskIOScheduler.getSlice(HARD_DELETE_CLEANUP_JOB_NAME, HARD_DELETE_CLEANUP_JOB_NAME, logWriteInfo.size);
        logWriteInfo.logSegment.writeFrom(logWriteInfo.channel, logWriteInfo.offset, logWriteInfo.size);
        metrics.hardDeleteDoneCount.inc(1);
      }
    } catch (IOException e) {
      StoreErrorCodes errorCode = StoreException.resolveErrorCode(e);
      throw new StoreException(errorCode.toString() + " while performing hard delete ", e, errorCode);
    }
    logger.trace("Performed hard deletes from {} to {} for {}", startToken, endToken, dataDir);
  }

  /**
   * A class to hold the information required to write hard delete stream to the Log.
   */
  private class LogWriteInfo {
    final LogSegment logSegment;
    final ReadableByteChannel channel;
    final long offset;
    final long size;

    LogWriteInfo(LogSegment logSegment, ReadableByteChannel channel, long offset, long size) {
      this.logSegment = logSegment;
      this.channel = channel;
      this.offset = offset;
      this.size = size;
    }
  }

  /**
   * A class to encapsulates the item to be persisted in the file for recovery.
   * This class includes three piece of information:
   * 1. a {@link BlobReadOptions} for blob that is in process of being hard deleted.
   * 2. a byte array that contains serialized bytes of some extra information for hard delete.
   * 3. a {@link StoreFindToken} that represents the start token when finding this deleted entry from the index.
   */
  class HardDeletePersistItem {
    BlobReadOptions blobReadOptions;
    byte[] messagesStoreRecoveryInfo;
    // The startToken when this blob is fetched from index.
    StoreFindToken startTokenForBlobReadOptions;

    HardDeletePersistItem(BlobReadOptions blobReadOptions, byte[] messagesStoreRecoveryInfo, StoreFindToken token) {
      this.blobReadOptions = blobReadOptions;
      this.messagesStoreRecoveryInfo = messagesStoreRecoveryInfo;
      this.startTokenForBlobReadOptions = token;
    }

    HardDeletePersistItem(BlobReadOptions blobReadOptions, byte[] messagesStoreRecoveryInfo) {
      this(blobReadOptions, messagesStoreRecoveryInfo, null);
    }
  }

  /**
   * An object of this class contains all the information required for performing the hard delete recovery for the
   * associated blob. This is the information that is persisted from time to time.
   */
  class HardDeletePersistInfo {
    private List<HardDeletePersistItem> items;

    HardDeletePersistInfo() {
      this.items = new ArrayList<>();
    }

    HardDeletePersistInfo(DataInputStream stream, StoreKeyFactory storeKeyFactory) throws IOException {
      this();
      int numBlobsToRecover = stream.readInt();
      List<BlobReadOptions> blobReadOptionsList = new ArrayList<>();
      List<byte[]> messageStoreRecoveryInfoList = new ArrayList<>();
      for (int i = 0; i < numBlobsToRecover; i++) {
        blobReadOptionsList.add(BlobReadOptions.fromBytes(stream, storeKeyFactory, log));
      }

      for (int i = 0; i < numBlobsToRecover; i++) {
        int lengthOfRecoveryInfo = stream.readInt();
        byte[] messageStoreRecoveryInfo = new byte[lengthOfRecoveryInfo];
        if (stream.read(messageStoreRecoveryInfo) != lengthOfRecoveryInfo) {
          throw new IOException("Token file could not be read correctly");
        }
        messageStoreRecoveryInfoList.add(messageStoreRecoveryInfo);
      }

      for (int i = 0; i < numBlobsToRecover; i++) {
        items.add(new HardDeletePersistItem(blobReadOptionsList.get(i), messageStoreRecoveryInfoList.get(i)));
      }
    }

    void addMessageInfo(BlobReadOptions blobReadOptions, byte[] messageStoreRecoveryInfo, StoreFindToken token) {
      items.add(new HardDeletePersistItem(blobReadOptions, messageStoreRecoveryInfo, token));
    }

    void clear() {
      items.clear();
    }

    int getSize() {
      return items.size();
    }

    /**
     * @return A serialized byte array containing the information required for hard delete recovery.
     */
    byte[] toBytes() throws IOException {
      ByteArrayOutputStream outStream = new ByteArrayOutputStream();
      DataOutputStream dataOutputStream = new DataOutputStream(outStream);

      /* Write the number of entries */
      dataOutputStream.writeInt(items.size());

      /* Write all the blobReadOptions */
      for (HardDeletePersistItem item : items) {
        dataOutputStream.write(item.blobReadOptions.toBytes());
      }

      /* Write all the messageStoreRecoveryInfos */
      for (HardDeletePersistItem item : items) {
        /* First write the size of the recoveryInfo */
        dataOutputStream.writeInt(item.messagesStoreRecoveryInfo.length);

        /* Now, write the recoveryInfo */
        dataOutputStream.write(item.messagesStoreRecoveryInfo);
      }

      return outStream.toByteArray();
    }

    /**
     * Prunes entries in the range from the start up to, but excluding, the entry with the passed in token.
     * Token passed to this method has to be a indexed based one.
     */
    void pruneTill(StoreFindToken token) {
      Iterator<HardDeletePersistItem> itemsIterator = items.iterator();
      while (itemsIterator.hasNext()) {
        HardDeletePersistItem item = itemsIterator.next();
        if (item.startTokenForBlobReadOptions.getType() == FindTokenType.Uninitialized) {
          itemsIterator.remove();
        } else if (item.startTokenForBlobReadOptions.getType() == FindTokenType.JournalBased) {
          // If the startToken for this blobReadOptions is JournalBased, then just don't remove it.
          // This has a potential consequence that the HardDeletePersistItem list would grow to infinite
          // since as HardDeleter moves through the PersistentIndex, it will points to Journal in the end
          // and always stay in Journal.
          // The solution to this problem is to force HardDeleter thread to sleep longer so the IndexPersistor
          // thread in PersistentIndex would be able to call preLogFlush and postLogFlush with startToken being
          // equal to endToken. For instance
          // 1. HardDeleter performs a hard delete, then the startToken is set to be endToken and it sleeps
          //    for 1 minutes
          // 2. Within this one minute, IndexPersistor thread calls preLogFlush, logFlush and postLogFlush,
          //    and since the startToken == endToken, the HardDeletePersistItem will be cleared.
          break;
        } else {
          if (compareTwoTokens(item.startTokenForBlobReadOptions, token) >= 0) {
            break;
          } else {
            itemsIterator.remove();
          }
        }
      }
    }

    /**
     * Compare two StoreFindTokens and return the result as an integer like compareTo interface.
     * These two tokens have to be IndexBased tokens.
     * @param token1 The first token to compare.
     * @param token2 The second tokent to compare.
     * @return 0 means they are equal. negative number means token1 is less than token2. postive number means the opposite.
     */
    int compareTwoTokens(StoreFindToken token1, StoreFindToken token2) {
      if (token1.getType() != FindTokenType.IndexBased || token2.getType() != FindTokenType.IndexBased) {
        throw new IllegalStateException(
            "At store " + dataDir + " Tokens need to be index based. First: " + token1 + " Second: " + token2);
      }
      if (token1.equals(token2)) {
        return 0;
      }
      if (!token1.getOffset().equals(token2.getOffset())) {
        return token1.getOffset().compareTo(token2.getOffset());
      }
      return token1.getStoreKey().compareTo(token2.getStoreKey());
    }

    private List<HardDeletePersistItem> getHardDeletePersistItemList() {
      return items;
    }
  }

  /**
   * @param dataDir the directory of the store for which the hard delete thread will be created
   * @return the name of the hard delete thread
   */
  static String getThreadName(String dataDir) {
    return "hard delete thread " + dataDir;
  }
}
