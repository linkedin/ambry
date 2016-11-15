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
import com.github.ambry.utils.CrcInputStream;
import com.github.ambry.utils.CrcOutputStream;
import com.github.ambry.utils.Throttler;
import com.github.ambry.utils.Time;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class that provides functionality to perform hard deletes.
 */
public class HardDeleter implements Runnable {

  public static final short Cleanup_Token_Version_V1 = 0;
  private static final String Cleanup_Token_Filename = "cleanuptoken";

  final AtomicBoolean running = new AtomicBoolean(true);

  private final StoreMetrics metrics;
  private final String dataDir;
  private final Log log;
  private final PersistentIndex index;
  private final MessageStoreHardDelete hardDelete;
  private final StoreKeyFactory factory;
  private final Time time;
  private final int scanSizeInBytes;
  private final int messageRetentionSeconds;
  // TODO (HardDelete Changes): This will stay until the HardDelete is rewritten to handle multiple segments.
  private final LogSegment logSegment;
  //how long to sleep if token does not advance.
  private final long hardDeleterSleepTimeWhenCaughtUpMs = 10 * Time.MsPerSec;
  private final CountDownLatch shutdownLatch = new CountDownLatch(1);
  private final Object lock = new Object();
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
  private FindToken startToken;
  private FindToken startTokenBeforeLogFlush;
  private FindToken startTokenSafeToPersist;
  private FindToken endToken;
  private StoreFindToken recoveryEndToken;
  private HardDeletePersistInfo hardDeleteRecoveryRange = new HardDeletePersistInfo();
  private Throttler throttler;
  boolean isCaughtUp = false;

  HardDeleter(StoreConfig config, StoreMetrics metrics, String dataDir, Log log, PersistentIndex index,
      MessageStoreHardDelete hardDelete, StoreKeyFactory factory, Time time) {
    this.metrics = metrics;
    this.dataDir = dataDir;
    this.log = log;
    logSegment = log.getFirstSegment();
    this.index = index;
    this.hardDelete = hardDelete;
    this.factory = factory;
    this.time = time;
    throttler = new Throttler(config.storeHardDeleteBytesPerSec, 10, true, time);
    scanSizeInBytes = config.storeHardDeleteBytesPerSec * 10;
    messageRetentionSeconds = config.storeDeletedMessageRetentionDays * Time.SecsPerDay;
  }

  @Override
  public void run() {
    try {
      while (running.get()) {
        try {
          if (!hardDelete()) {
            isCaughtUp = true;
            synchronized (lock) {
              if (!running.get()) {
                break;
              }
              time.wait(lock, hardDeleterSleepTimeWhenCaughtUpMs);
            }
          } else if (isCaughtUp) {
            isCaughtUp = false;
            logger.info("Resumed hard deletes for {} after having caught up", dataDir);
          }
        } catch (StoreException e) {
          if (e.getErrorCode() != StoreErrorCodes.Store_Shutting_Down) {
            logger.error("Caught store exception: ", e);
          } else {
            logger.trace("Caught exception during hard deletes", e);
          }
        } catch (InterruptedException e) {
          logger.trace("Caught exception during hard deletes", e);
        }
      }
    } finally {
      close();
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
      List<BlobReadOptions> readOptionsList = hardDeleteRecoveryRange.getBlobReadOptionsList();

        /* Next, perform the log write. The token file does not have to be persisted again as only entries that are
           currently in it are being hard deleted as part of recovery. */
      StoreMessageReadSet readSet = log.getView(readOptionsList);
      Iterator<HardDeleteInfo> hardDeleteIterator =
          hardDelete.getHardDeleteMessages(readSet, factory, hardDeleteRecoveryRange.getMessageStoreRecoveryInfoList());

      Iterator<BlobReadOptions> readOptionsIterator = readOptionsList.iterator();
      while (hardDeleteIterator.hasNext()) {
        if (!running.get()) {
          throw new StoreException("Aborting hard deletes as store is shutting down",
              StoreErrorCodes.Store_Shutting_Down);
        }
        HardDeleteInfo hardDeleteInfo = hardDeleteIterator.next();
        BlobReadOptions readOptions = readOptionsIterator.next();
        if (hardDeleteInfo == null) {
          metrics.hardDeleteFailedCount.inc(1);
        } else {
          logSegment.writeFrom(hardDeleteInfo.getHardDeleteChannel(),
              readOptions.getOffset() + hardDeleteInfo.getStartOffsetInMessage(),
              hardDeleteInfo.getHardDeletedMessageSize());
          metrics.hardDeleteDoneCount.inc(1);
        }
      }
    } catch (IOException e) {
      metrics.hardDeleteExceptionsCount.inc();
      throw new StoreException("IO exception while performing hard delete ", e, StoreErrorCodes.IOError);
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
    if (logFlushedTillToken != null && !logFlushedTillToken.isUninitialized()) {
      if (logFlushedTillToken.equals(endToken)) {
        hardDeleteRecoveryRange.clear();
      } else if (logFlushedTillToken.getStoreKey() != null) {
          /* Avoid pruning if the token is journal based as it is complicated and unnecessary. This is because, in the
             recovery range, keys are stored not offsets. It should be okay to not prune though, as there is no
             correctness issue. Additionally, since this token is journal based, it is highly likely that this token
             will soon become equal to endtoken, in which case we will prune everything (in the "if case" above).
             If the token is index based, safely prune off entries that have already been flushed in the log */
        hardDeleteRecoveryRange.pruneTill(logFlushedTillToken.getStoreKey());
      }
    }
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
    if (index.getCurrentEndOffset() > 0) {
      final Timer.Context context = metrics.hardDeleteTime.time();
      try {
        FindInfo info =
            index.findDeletedEntriesSince(startToken, scanSizeInBytes, time.seconds() - messageRetentionSeconds);
        endToken = info.getFindToken();
        pruneHardDeleteRecoveryRange();
        if (!endToken.equals(startToken)) {
          if (!info.getMessageEntries().isEmpty()) {
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
   */
  void preLogFlush() {
      /* Save the current start token before the log gets flushed */
    startTokenBeforeLogFlush = startToken;
  }

  /**
   * This method will be called after the log is flushed.
   */
  void postLogFlush() {
      /* start token saved before the flush is now safe to be persisted */
    startTokenSafeToPersist = startTokenBeforeLogFlush;
  }

  /**
   * Gets the number of bytes processed so far
   * @return the number of bytes processed so far as represented by the start token. Note that if the token is
   * index based, this is at segment granularity.
   */
  long getProgress() {
    StoreFindToken token = (StoreFindToken) startToken;
    if (token.isUninitialized()) {
      return 0;
    } else if (token.getOffset() != StoreFindToken.Uninitialized_Offset) {
      return token.getOffset();
    } else {
      return token.getIndexStartOffset();
    }
  }

  /**
   * Returns true if the hard delete is currently running.
   * @return true if running, false otherwise.
   */
  public boolean isRunning() {
    return shutdownLatch.getCount() != 0;
  }

  /**
   * Returns true if the hard delete thread has caught up, that is if the token did not advance in the last iteration
   * @return true if caught up, false otherwise.
   */
  boolean isCaughtUp() {
    return isCaughtUp;
  }

  void shutdown() throws InterruptedException, StoreException, IOException {
    if (running.get()) {
      running.set(false);
      synchronized (lock) {
        lock.notify();
      }
      throttler.close();
      shutdownLatch.await();
      pruneHardDeleteRecoveryRange();
      persistCleanupToken();
    }
  }

  void close() {
    running.set(false);
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
          case Cleanup_Token_Version_V1:
            recoveryStartToken = StoreFindToken.fromBytes(stream, factory);
            recoveryEndToken = StoreFindToken.fromBytes(stream, factory);
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
           --
           crc
           ---
         */
    if (endToken == null || ((StoreFindToken) endToken).isUninitialized()) {
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
      writer.write(hardDeleteRecoveryRange.toBytes());
      long crcValue = crc.getValue();
      writer.writeLong(crcValue);

      fileStream.getChannel().force(true);
      tempFile.renameTo(actual);
    } catch (IOException e) {
      throw new StoreException("IO error while persisting cleanup tokens to disk " + tempFile.getAbsoluteFile(),
          StoreErrorCodes.IOError);
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
        if (!running.get()) {
          throw new StoreException("Aborting, store is shutting down", StoreErrorCodes.Store_Shutting_Down);
        }
        try {
          BlobReadOptions readInfo = index.getBlobReadInfo(info.getStoreKey(), getOptions);
          readOptionsList.add(readInfo);
        } catch (StoreException e) {
          logger.error("Failed to read blob info for blobid {} during hard deletes, ignoring. Caught exception {}",
              info.getStoreKey(), e);
          metrics.hardDeleteExceptionsCount.inc();
        }
      }

      List<LogWriteInfo> logWriteInfoList = new ArrayList<LogWriteInfo>();

      StoreMessageReadSet readSet = log.getView(readOptionsList);
      Iterator<HardDeleteInfo> hardDeleteIterator = hardDelete.getHardDeleteMessages(readSet, factory, null);
      Iterator<BlobReadOptions> readOptionsIterator = readOptionsList.iterator();

        /* Next, get the information to persist hard delete recovery info. Get all the information and save it, as only
         * after the whole range is persisted can we start with the actual log write */
      while (hardDeleteIterator.hasNext()) {
        if (!running.get()) {
          throw new StoreException("Aborting hard deletes as store is shutting down",
              StoreErrorCodes.Store_Shutting_Down);
        }
        HardDeleteInfo hardDeleteInfo = hardDeleteIterator.next();
        BlobReadOptions readOptions = readOptionsIterator.next();
        if (hardDeleteInfo == null) {
          metrics.hardDeleteFailedCount.inc(1);
        } else {
          hardDeleteRecoveryRange.addMessageInfo(readOptions, hardDeleteInfo.getRecoveryInfo());
          logWriteInfoList.add(new LogWriteInfo(hardDeleteInfo.getHardDeleteChannel(),
              readOptions.getOffset() + hardDeleteInfo.getStartOffsetInMessage(),
              hardDeleteInfo.getHardDeletedMessageSize()));
        }
      }

      if (readOptionsIterator.hasNext()) {
        metrics.hardDeleteExceptionsCount.inc(1);
        throw new IllegalStateException("More number of blobReadOptions than hardDeleteMessages");
      }

      persistCleanupToken();

        /* Finally, write the hard delete stream into the Log */
      for (LogWriteInfo logWriteInfo : logWriteInfoList) {
        if (!running.get()) {
          throw new StoreException("Aborting hard deletes as store is shutting down",
              StoreErrorCodes.Store_Shutting_Down);
        }

        logSegment.writeFrom(logWriteInfo.channel, logWriteInfo.offset, logWriteInfo.size);
        metrics.hardDeleteDoneCount.inc(1);
        throttler.maybeThrottle(logWriteInfo.size);
      }
    } catch (InterruptedException e) {
      if (running.get()) {
        // We throw here because we do not want the tokens to be updated.
        throw new StoreException("Got interrupted during hard deletes", StoreErrorCodes.Unknown_Error);
      } else {
        throw new StoreException("Got interrupted as store is shutting down", StoreErrorCodes.Store_Shutting_Down);
      }
    } catch (IOException e) {
      throw new StoreException("IO exception while performing hard delete ", e, StoreErrorCodes.IOError);
    }
  }

  /**
   * A class to hold the information required to write hard delete stream to the Log.
   */
  private class LogWriteInfo {
    ReadableByteChannel channel;
    long offset;
    long size;

    LogWriteInfo(ReadableByteChannel channel, long offset, long size) {
      this.channel = channel;
      this.offset = offset;
      this.size = size;
    }
  }

  /**
   * An object of this class contains all the information required for performing the hard delete recovery for the
   * associated blob. This is the information that is persisted from time to time.
   */
  private class HardDeletePersistInfo {
    private List<BlobReadOptions> blobReadOptionsList;
    private List<byte[]> messageStoreRecoveryInfoList;

    HardDeletePersistInfo() {
      this.blobReadOptionsList = new ArrayList<BlobReadOptions>();
      this.messageStoreRecoveryInfoList = new ArrayList<byte[]>();
    }

    HardDeletePersistInfo(DataInputStream stream, StoreKeyFactory storeKeyFactory) throws IOException {
      this();
      int numBlobsToRecover = stream.readInt();
      for (int i = 0; i < numBlobsToRecover; i++) {
        blobReadOptionsList.add(BlobReadOptions.fromBytes(stream, storeKeyFactory));
      }

      for (int i = 0; i < numBlobsToRecover; i++) {
        int lengthOfRecoveryInfo = stream.readInt();
        byte[] messageStoreRecoveryInfo = new byte[lengthOfRecoveryInfo];
        if (stream.read(messageStoreRecoveryInfo) != lengthOfRecoveryInfo) {
          throw new IOException("Token file could not be read correctly");
        }
        messageStoreRecoveryInfoList.add(messageStoreRecoveryInfo);
      }
    }

    void addMessageInfo(BlobReadOptions blobReadOptions, byte[] messageStoreRecoveryInfo) {
      this.blobReadOptionsList.add(blobReadOptions);
      this.messageStoreRecoveryInfoList.add(messageStoreRecoveryInfo);
    }

    void clear() {
      blobReadOptionsList.clear();
      messageStoreRecoveryInfoList.clear();
    }

    int getSize() {
      return blobReadOptionsList.size();
    }

    /**
     * @return A serialized byte array containing the information required for hard delete recovery.
     */
    byte[] toBytes() throws IOException {
      ByteArrayOutputStream outStream = new ByteArrayOutputStream();
      DataOutputStream dataOutputStream = new DataOutputStream(outStream);

      /* Write the number of entries */
      dataOutputStream.writeInt(blobReadOptionsList.size());

      /* Write all the blobReadOptions */
      for (BlobReadOptions blobReadOptions : blobReadOptionsList) {
        dataOutputStream.write(blobReadOptions.toBytes());
      }

      /* Write all the messageStoreRecoveryInfos */
      for (byte[] recoveryInfo : messageStoreRecoveryInfoList) {
        /* First write the size of the recoveryInfo */
        dataOutputStream.writeInt(recoveryInfo.length);

        /* Now, write the recoveryInfo */
        dataOutputStream.write(recoveryInfo);
      }

      return outStream.toByteArray();
    }

    /**
     * Prunes entries in the range from the start up to, but excluding, the entry with the passed in key.
     */
    void pruneTill(StoreKey storeKey) {
      Iterator<BlobReadOptions> blobReadOptionsListIterator = blobReadOptionsList.iterator();
      Iterator<byte[]> messageStoreRecoveryListIterator = messageStoreRecoveryInfoList.iterator();
      while (blobReadOptionsListIterator.hasNext()) {
      /* Note: In the off chance that there are multiple presence of the same key in this range due to prior software
         bugs, note that this method prunes only till the first occurrence of the key. If it so happens that a
         later occurrence is the one really associated with this token, it does not affect the safety.
         Persisting more than what is required is okay as hard deleting a blob is an idempotent operation. */
        messageStoreRecoveryListIterator.next();
        if (blobReadOptionsListIterator.next().getStoreKey().equals(storeKey)) {
          break;
        } else {
          blobReadOptionsListIterator.remove();
          messageStoreRecoveryListIterator.remove();
        }
      }
    }

    private List<BlobReadOptions> getBlobReadOptionsList() {
      return blobReadOptionsList;
    }

    private List<byte[]> getMessageStoreRecoveryInfoList() {
      return messageStoreRecoveryInfoList;
    }
  }
}
