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

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.replication.FindToken;
import com.github.ambry.replication.FindTokenType;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Tests for {@link HardDeleter}.
 */
public class HardDeleterTest {

  private class HardDeleteTestHelper implements MessageStoreHardDelete {
    private long nextOffset;
    private long sizeOfEntry;
    private MockIndex index;
    private Log log;
    private String logSegmentName;
    HashMap<Long, MessageInfo> offsetMap;
    IOException exception;

    HardDeleteTestHelper(long offset, long size) {
      nextOffset = offset;
      sizeOfEntry = size;
      offsetMap = new HashMap<>();
    }

    void setIndex(MockIndex index, Log log) {
      this.index = index;
      this.log = log;
      logSegmentName = log.getFirstSegment().getName();
    }

    void add(MockId id) throws StoreException {
      Offset offset = new Offset(logSegmentName, nextOffset);
      short acccountId = Utils.getRandomShort(TestUtils.RANDOM);
      short containerId = Utils.getRandomShort(TestUtils.RANDOM);
      IndexValue indexValue =
          new IndexValue(sizeOfEntry, offset, IndexValue.FLAGS_DEFAULT_VALUE, time.milliseconds() + 12345,
              time.milliseconds(), acccountId, containerId, (short) 0);
      index.addToIndex(new IndexEntry(id, indexValue),
          new FileSpan(offset, new Offset(logSegmentName, nextOffset + sizeOfEntry)));
      ByteBuffer byteBuffer = ByteBuffer.allocate((int) sizeOfEntry);
      byteBuffer.put(TestUtils.getRandomBytes((int) sizeOfEntry));
      byteBuffer.flip();
      log.appendFrom(byteBuffer);
      offsetMap.put(nextOffset, new MessageInfo(id, sizeOfEntry, acccountId, containerId, time.milliseconds()));
      nextOffset += sizeOfEntry;
    }

    void delete(MockId id) throws StoreException {
      Offset offset = new Offset(logSegmentName, nextOffset);
      index.markAsDeleted(id, new FileSpan(offset, new Offset(logSegmentName, nextOffset + sizeOfEntry)),
          time.milliseconds());
      ByteBuffer byteBuffer = ByteBuffer.allocate((int) sizeOfEntry);
      log.appendFrom(byteBuffer);
      nextOffset += sizeOfEntry;
    }

    void undelete(MockId id) throws StoreException {
      Offset offset = new Offset(logSegmentName, nextOffset);
      index.markAsUndeleted(id, new FileSpan(offset, new Offset(logSegmentName, nextOffset + sizeOfEntry)),
          time.milliseconds());
      ByteBuffer byteBuffer = ByteBuffer.allocate((int) sizeOfEntry);
      log.appendFrom(byteBuffer);
      nextOffset += sizeOfEntry;
    }

    ByteBuffer readRecord(StoreKey key) throws IOException, StoreException {
      BlobReadOptions readOptions = index.getBlobReadInfo(key, EnumSet.allOf(StoreGetOptions.class));
      readOptions.doPrefetch(0, readOptions.getMessageInfo().getSize());
      return readOptions.getPrefetchedData();
    }

    @Override
    public Iterator<HardDeleteInfo> getHardDeleteMessages(MessageReadSet readSet, StoreKeyFactory factory,
        List<byte[]> recoveryInfoList) throws IOException {
      if (exception != null) {
        throw exception;
      }
      class MockMessageStoreHardDeleteIterator implements Iterator<HardDeleteInfo> {
        int count;
        MessageReadSet readSet;

        MockMessageStoreHardDeleteIterator(MessageReadSet readSet) {
          this.readSet = readSet;
          this.count = readSet.count();
        }

        @Override
        public boolean hasNext() {
          return count > 0;
        }

        @Override
        public HardDeleteInfo next() {
          if (!hasNext()) {
            throw new NoSuchElementException();
          }
          count--;
          ByteBuffer buf = ByteBuffer.allocate(100);
          byte[] recoveryInfo = new byte[100];
          Arrays.fill(recoveryInfo, (byte) 0);
          buf.put(recoveryInfo);
          buf.flip();
          ByteBufferInputStream stream = new ByteBufferInputStream(buf);
          ReadableByteChannel channel = Channels.newChannel(stream);
          HardDeleteInfo hardDeleteInfo = new HardDeleteInfo(channel, 100, 100, recoveryInfo);
          return hardDeleteInfo;
        }

        @Override
        public void remove() {
          throw new UnsupportedOperationException();
        }
      }

      return new MockMessageStoreHardDeleteIterator(readSet);
    }
  }

  private MockIndex index = null;
  private HardDeleteTestHelper helper = null;
  private MockTime time = null;
  private ScheduledExecutorService scheduler;
  private Log log;
  private MockDiskIOScheduler diskIOScheduler = null;

  @Before
  public void setup() throws Exception {
    File rootDirectory = StoreTestUtils.createTempDirectory("ambry");
    File indexFile = new File(rootDirectory.getAbsolutePath());
    for (File c : indexFile.listFiles()) {
      c.delete();
    }
    scheduler = Utils.newScheduler(1, false);
    MetricRegistry metricRegistry = new MetricRegistry();
    StoreMetrics metrics = new StoreMetrics(metricRegistry);
    Properties props = new Properties();
    // the test will set the tokens, so disable the index persistor.
    props.setProperty("store.data.flush.interval.seconds", "3600");
    props.setProperty("store.deleted.message.retention.days", "1");
    props.setProperty("store.index.max.number.of.inmem.elements", "2");
    props.setProperty("store.segment.size.in.bytes", "10000");
    // the following determines the number of entries that will be fetched at most. We need this to test the
    // case where the endToken does not reach the journal.
    props.setProperty("store.hard.delete.operations.bytes.per.sec", "40");
    StoreConfig config = new StoreConfig(new VerifiableProperties(props));
    log = new Log(rootDirectory.getAbsolutePath(), 10000, StoreTestUtils.DEFAULT_DISK_SPACE_ALLOCATOR, config, metrics);
    StoreKeyFactory factory = Utils.getObj("com.github.ambry.store.MockIdFactory");
    time = new MockTime(SystemTime.getInstance().milliseconds());

    helper = new HardDeleteTestHelper(0, 200);
    diskIOScheduler = new MockDiskIOScheduler();
    index =
        new MockIndex(rootDirectory.getAbsolutePath(), scheduler, log, config, factory, helper, diskIOScheduler, time,
            UUID.randomUUID());
    helper.setIndex(index, log);
    // Setting this below will not enable the hard delete thread. This being a unit test, the methods
    // are going to be called directly. We simply want to set the enabled flag to avoid those methods
    // from bailing out prematurely.
    index.setHardDeleteRunningStatus(true);
  }

  @After
  public void cleanup() throws StoreException, IOException {
    scheduler.shutdown();
    index.close(false);
    log.close(false);
  }

  /**
   * Test hard delete and recovery failure due to IOExceptions.
   * @throws Exception
   */
  @Test
  public void performHardDeleteAndRecoveryFailureTest() throws Exception {
    MockId blobId01 = new MockId("id01");
    MockId blobId02 = new MockId("id02");
    MockId blobId03 = new MockId("id03");
    MockId blobId04 = new MockId("id04");

    helper.add(blobId01);
    helper.add(blobId02);
    helper.add(blobId03);
    helper.add(blobId04);
    helper.delete(blobId03);
    time.sleep(TimeUnit.DAYS.toMillis(2));
    helper.exception = new IOException(StoreException.IO_ERROR_STR);
    try {
      index.hardDelete();
      index.persistAndAdvanceStartTokenSafeToPersist();
      index.hardDelete();
      index.hardDelete();
      fail("Hard delete should fail due to I/O error");
    } catch (StoreException e) {
      assertEquals("Mismatch in error code", StoreErrorCodes.IOError, e.getErrorCode());
    }
    helper.exception = null;
    index.hardDelete();
    helper.exception = new IOException(StoreException.IO_ERROR_STR);
    try {
      index.performHardDeleteRecovery();
      fail("Recovery should fail due to I/O error");
    } catch (StoreException e) {
      assertEquals("Mismatch in error code", StoreErrorCodes.IOError, e.getErrorCode());
    }
  }

  @Test
  public void testHardDelete() {
    // Create a mock index with regular log.
    // perform puts to the index.
    // perform deletes to the index.
    // call hardDelete() explicitly (set the thread frequency time to a large number if required) -
    // that will do findDeleted, prune and performHardDelete()
    // perform will need a messageStoreHardDelete implementation that just returns something
    // that will be written back. - need to implement that.
    // disable index persistor if that can be done.
    // call persist cleanup token and close the index. Reopen it to execute recovery path.
    // perform hard deletes upto some point.
    // perform more deletes.
    // do recovery.
    try {
      List<MockId> keys = setupIndexForTest();
      boolean recordContentChecked = false;
      for (int i = 0; i < 3; i++) {
        assertEquals("There should have been no progress reported for hard deletes", 0, index.getHardDeleteProgress());
        // The first * shows where startTokenSafeToPersist is
        // The second * shows where startToken/endToken are before the operations.
        // Note since we are only depicting values before and after hardDelete() is done, startToken and endToken
        // will be the same.
        StoreFindToken token = (StoreFindToken) index.hardDeleter.getStartTokenSafeToPersistTo();
        assertEquals("Token type mismatch", token.getType(), FindTokenType.Uninitialized);
        Set<StoreKey> hardDeletedKeys = new HashSet<>();

        // indexes: **[1 2] [3 4] [3d 5] [6 7] [2d 6d] [8 9] [1d 10] [1u 3u] [8d 10d]
        // journal:                                                    [3u 1u 10d 8d]
        boolean tokenMovedForward = index.hardDelete();
        Assert.assertTrue(tokenMovedForward);
        if (!recordContentChecked) {
          checkRecordHardDeleted(keys, hardDeletedKeys);
        }
        assertEquals("Recovery range size mismatch", 0, index.hardDeleter.getHardDeleteRecoveryRange().getSize());
        // startToken = endToken = 2.

        // call into the log flush hooks so that startTokenSafeToPersist = startToken = 2.
        index.persistAndAdvanceStartTokenSafeToPersist();
        token = (StoreFindToken) index.hardDeleter.getStartTokenSafeToPersistTo();
        assertEquals("Token type mismatch", token.getType(), FindTokenType.IndexBased);
        assertEquals("Token key mismatch", token.getStoreKey(), keys.get(1)); // blobId02
        // Token still points to the first index segment, and the start offset of the first segment is 0
        assertEquals("Token offset mismatch", token.getOffset().getOffset(), 0);

        // indexes: [1 2**] [3 4] [3d 5] [6 7] [2d 6d] [8 9] [1d 10] [1u 3u] [8d 10d]
        // journal:                                                    [3u 1u 10d 8d]
        tokenMovedForward = index.hardDelete();
        Assert.assertTrue(tokenMovedForward);
        if (!recordContentChecked) {
          checkRecordHardDeleted(keys, hardDeletedKeys);
        }
        assertEquals("Recovery range size mismatch", 0, index.hardDeleter.getHardDeleteRecoveryRange().getSize());

        // Without moving the token forward, make sure it's still the old value.
        token = (StoreFindToken) index.hardDeleter.getStartTokenSafeToPersistTo();
        assertEquals("Token type mismatch", token.getType(), FindTokenType.IndexBased);
        assertEquals("Token key mismatch", token.getStoreKey(), keys.get(1)); // blobId02
        assertEquals("Token offset mismatch", token.getOffset().getOffset(), 0);
        // startToken = endToken = 4.

        // indexes: [1 2*] [3 4*] [3d 5] [6 7] [2d 6d] [8 9] [1d 10] [1u 3u] [8d 10d]
        // journal:                                                    [3u 1u 10d 8d]
        tokenMovedForward = index.hardDelete();
        Assert.assertTrue(tokenMovedForward);
        // blob03 is deleted in this index segment, but it's undeleted later.
        if (!recordContentChecked) {
          checkRecordHardDeleted(keys, hardDeletedKeys);
        }
        assertEquals("Recovery range size mismatch", 0, index.hardDeleter.getHardDeleteRecoveryRange().getSize());
        // startToken = 5, endToken = 5, startTokenSafeToPersist = 2

        // indexes: [1 2*] [3 4] [3d 5*] [6 7] [2d 6d] [8 9] [1d 10] [1u 3u] [8d 10d]
        // journal:                                                    [3u 1u 10d 8d]
        index.persistAndAdvanceStartTokenSafeToPersist();
        token = (StoreFindToken) index.hardDeleter.getStartTokenSafeToPersistTo();
        assertEquals("Token type mismatch", token.getType(), FindTokenType.IndexBased);
        assertEquals("Token key mismatch", token.getStoreKey(), keys.get(4)); // blobId05
        // Token moves to third segment, the start offset of the third segment is 4 * sizeOfEntry
        assertEquals("Token offset mismatch", token.getOffset().getOffset(), 2 * 2 * helper.sizeOfEntry);
        tokenMovedForward = index.hardDelete();
        Assert.assertTrue(tokenMovedForward);
        if (!recordContentChecked) {
          checkRecordHardDeleted(keys, hardDeletedKeys);
        }
        assertEquals("Recovery range size mismatch", 0, index.hardDeleter.getHardDeleteRecoveryRange().getSize());
        // startToken = 7, endToken = 7, starttokenSafeToPersist = 5
        // 3d just got pruned.

        // indexes: [1 2] [3 4] [3d 5*] [6 7*] [2d 6d] [8 9] [1d 10] [1u 3u] [8d 10d]
        // journal:                                                    [3u 1u 10d 8d]
        tokenMovedForward = index.hardDelete();
        Assert.assertTrue(tokenMovedForward);
        hardDeletedKeys.add(keys.get(1)); // blobId02
        hardDeletedKeys.add(keys.get(5)); // blobId06
        if (!recordContentChecked) {
          checkRecordHardDeleted(keys, hardDeletedKeys);
        }
        // blobid02 and blobid06
        assertEquals("Recovery range size mismatch", 2, index.hardDeleter.getHardDeleteRecoveryRange().getSize());

        // indexes: [1 2] [3 4] [3d 5*] [6 7] [2d 6d*] [8 9] [1d 10] [1u 3u] [8d 10d]
        // journal:                                                    [3u 1u 10d 8d]
        tokenMovedForward = index.hardDelete();
        Assert.assertTrue(tokenMovedForward);
        if (!recordContentChecked) {
          checkRecordHardDeleted(keys, hardDeletedKeys);
        }
        // Didn't perform hard delete, so blobid02 and blobid06
        assertEquals("Recovery range size mismatch", 2, index.hardDeleter.getHardDeleteRecoveryRange().getSize());

        // indexes: [1 2] [3 4] [3d 5*] [6 7] [2d 6d] [8 9*] [1d 10] [1u 3u] [8d 10d]
        // journal:                                                    [3u 1u 10d 8d]
        index.persistAndAdvanceStartTokenSafeToPersist();
        token = (StoreFindToken) index.hardDeleter.getStartTokenSafeToPersistTo();
        assertEquals("Token type mismatch", token.getType(), FindTokenType.IndexBased);
        assertEquals("Token key mismatch", token.getStoreKey(), keys.get(8)); // blobId09
        assertEquals("Token offset mismatch", token.getOffset().getOffset(), 5 * 2 * helper.sizeOfEntry);
        tokenMovedForward = index.hardDelete();
        Assert.assertTrue(tokenMovedForward);
        // blob01 is deleted in this index segment, but it's undeleted later
        if (!recordContentChecked) {
          checkRecordHardDeleted(keys, hardDeletedKeys);
        }
        assertEquals("Recovery range size mismatch", 0, index.hardDeleter.getHardDeleteRecoveryRange().getSize());

        // indexes: [1 2] [3 4] [3d 5] [6 7] [2d 6d] [8 9*] [1d 10*] [1u 3u] [8d 10d]
        // journal:                                                    [3u 1u 10d 8d]
        tokenMovedForward = index.hardDelete();
        Assert.assertTrue(tokenMovedForward);
        if (!recordContentChecked) {
          checkRecordHardDeleted(keys, hardDeletedKeys);
        }

        // indexes: [1 2] [3 4] [3d 5] [6 7] [2d 6d] [8 9*] [1d 10] [1u 3u] [8d 10d]
        // journal:                                                    [3u 1u* 10d 8d]
        tokenMovedForward = index.hardDelete();
        Assert.assertTrue(tokenMovedForward);
        hardDeletedKeys.add(keys.get(9)); // blobId10
        hardDeletedKeys.add(keys.get(7)); // blobId08
        if (!recordContentChecked) {
          checkRecordHardDeleted(keys, hardDeletedKeys);
        }
        // blobid08 and blobid10
        assertEquals("Recovery range size mismatch", 2, index.hardDeleter.getHardDeleteRecoveryRange().getSize());
        // Persist safeToken here since now we have two items in the recovery range and their startToken is journal-based.
        token = (StoreFindToken) index.hardDeleter.getStartTokenSafeToPersistTo();
        index.hardDeleter.getHardDeleteRecoveryRange().pruneTill(token);
        // After prune we still have two
        assertEquals("Recovery range size mismatch", 2, index.hardDeleter.getHardDeleteRecoveryRange().getSize());

        // indexes: [1 2] [3 4] [3d 5] [6 7] [2d 6d] [8 9*] [1d 10] [1u 3u] [8d 10d]
        // journal:                                                    [3u 1u 10d 8d*]
        // All caught up, hardDelete should not move token forward.
        tokenMovedForward = index.hardDelete();
        Assert.assertFalse(tokenMovedForward);

        index.persistAndAdvanceStartTokenSafeToPersist();
        token = (StoreFindToken) index.hardDeleter.getStartTokenSafeToPersistTo();
        assertEquals("Token type mismatch", token.getType(), FindTokenType.JournalBased);
        // JournalBased Token is not inclusive, so offset pointing to 1u is the offset of 1u.
        assertEquals("Token offset mismatch", token.getOffset().getOffset(), 17 * helper.sizeOfEntry);
        assertEquals("Recovery range size mismatch", 0, index.hardDeleter.getHardDeleteRecoveryRange().getSize());

        // directly prune the recovery range completely (which should happen since we flushed till the endToken).
        index.pruneHardDeleteRecoveryRange();

        // Test recovery - this tests reading from the persisted token, filling up the hard delete recovery range and
        // then actually redoing the hard deletes on the range.
        index.performHardDeleteRecovery();

        // reset the internal tokens
        index.resetHardDeleterTokens();
        recordContentChecked = true;
      }
      index.close(false);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Unexpected exception");
    }
  }

  /**
   * Test for recovery in HardDeleter.
   * @throws Exception
   */
  @Test
  public void testHardDeleteRecovery() throws Exception {
    List<MockId> keys = setupIndexForTest();

    // indexes: **[1 2] [3 4] [3d 5] [6 7] [2d 6d] [8 9] [1d 10] [1u 3u] [8d 10d]
    // journal:                                                    [3u 1u 10d 8d]
    index.hardDelete();
    index.persistAndAdvanceStartTokenSafeToPersist();

    // indexes: [1 2**] [3 4] [3d 5] [6 7] [2d 6d] [8 9] [1d 10] [1u 3u] [8d 10d]
    // journal:                                                    [3u 1u 10d 8d]
    index.hardDelete();

    // indexes: [1 2*] [3 4*] [3d 5] [6 7] [2d 6d] [8 9] [1d 10] [1u 3u] [8d 10d]
    // journal:                                                    [3u 1u 10d 8d]
    index.hardDelete();
    index.persistAndAdvanceStartTokenSafeToPersist();

    // indexes: [1 2*] [3 4] [3d 5*] [6 7] [2d 6d] [8 9] [1d 10] [1u 3u] [8d 10d]
    // journal:                                                    [3u 1u 10d 8d]
    index.hardDelete();

    // Here we are going to perform a hard delete. We set the exception for diskIOScheduler so that we can persist
    // HardDeleteRecoveryRange, but not changing the log data. Since diskIOScheduler will be invoked before every log
    // write, we can use it to throw an exception and stop the writes from happening. Then we have to perform the recovery
    // and make sure recovery works.
    Set<StoreKey> hardDeletedKeys = new HashSet<>();

    // indexes: [1 2] [3 4] [3d 5*] [6 7*] [2d 6d] [8 9] [1d 10] [1u 3u] [8d 10d]
    // journal:                                                    [3u 1u 10d 8d]
    diskIOScheduler.setException(new IOException("Stopping log writes"));
    try {
      index.hardDelete();
      fail("Should throw an exception here");
    } catch (RuntimeException e) {
      // make sure we nothing is changed in the log
      checkRecordHardDeleted(keys, hardDeletedKeys);
    }

    // Now recovery from the "crash"
    diskIOScheduler.setException(null);
    hardDeletedKeys.add(keys.get(1)); // blobId02
    hardDeletedKeys.add(keys.get(5)); // blobId06
    index.performHardDeleteRecovery();
    checkRecordHardDeleted(keys, hardDeletedKeys);

    StoreFindToken startToken = (StoreFindToken) index.hardDeleter.getStartToken();
    StoreFindToken endToken = (StoreFindToken) index.hardDeleter.getEndToken();
    Assert.assertEquals("StartToken should match EndToken", startToken, endToken);
    assertEquals("Token type mismatch", startToken.getType(), FindTokenType.IndexBased);
    assertEquals("Token key mismatch", startToken.getStoreKey(), keys.get(5)); // blobId06
    assertEquals("Token offset mismatch", startToken.getOffset().getOffset(), 4 * 2 * helper.sizeOfEntry);

    // Now make sure we can move on with hard deleter.
    // After perform recovery, the start token is now equal to end token.
    // indexes: [1 2] [3 4] [3d 5] [6 7] [2d 6d**] [8 9] [1d 10] [1u 3u] [8d 10d]
    // journal:                                                    [3u 1u 10d 8d]
    index.hardDelete();

    // indexes: [1 2] [3 4] [3d 5] [6 7] [2d 6d*] [8 9*] [1d 10] [1u 3u] [8d 10d]
    // journal:                                                    [3u 1u 10d 8d]
    index.hardDelete();

    // inject this supplier to mimic the persistor thread to persist recovery range.
    // The reason to set it here and not above is the hardDelete method below would actually trigger a
    // change of recovery range.
    index.hardDeleter.setInjectedBeforeHardDeleteSupplier(() -> {
      index.persistAndAdvanceStartTokenSafeToPersist();
      return null;
    });
    // indexes: [1 2] [3 4] [3d 5] [6 7] [2d 6d*] [8 9] [1d 10*] [1u 3u] [8d 10d]
    // journal:                                                    [3u 1u 10d 8d]
    index.hardDelete();
    index.hardDeleter.setInjectedBeforeHardDeleteSupplier(null);

    // indexes: [1 2] [3 4] [3d 5] [6 7] [2d 6d*] [8 9] [1d 10] [1u 3u] [8d 10d]
    // journal:                                                    [3u 1u* 10d 8d]
    index.hardDelete();
    hardDeletedKeys.add(keys.get(9)); // blobId10
    hardDeletedKeys.add(keys.get(7)); // blobId08
    checkRecordHardDeleted(keys, hardDeletedKeys);
  }

  /**
   * Set up index for the test. This method would add some blobs, then deletes and undeletes some of them.
   * @return List of keys added to the index.
   * @throws Exception
   */
  private List<MockId> setupIndexForTest() throws Exception {
    MockId blobId01 = new MockId("id01");
    MockId blobId02 = new MockId("id02");
    MockId blobId03 = new MockId("id03");
    MockId blobId04 = new MockId("id04");
    MockId blobId05 = new MockId("id05");
    MockId blobId06 = new MockId("id06");
    MockId blobId07 = new MockId("id07");
    MockId blobId08 = new MockId("id08");
    MockId blobId09 = new MockId("id09");
    MockId blobId10 = new MockId("id10");
    List<MockId> keys = new ArrayList<>();
    keys.add(blobId01);
    keys.add(blobId02);
    keys.add(blobId03);
    keys.add(blobId04);
    keys.add(blobId05);
    keys.add(blobId06);
    keys.add(blobId07);
    keys.add(blobId08);
    keys.add(blobId09);
    keys.add(blobId10);

    // indexes: [1 2] [3 4] [3d 5] [6 7] [2d 6d] [8 9] [1d 10] [1u 3u] [8d 10d]
    // journal:                                                  [3u 1u 10d 8d]

    helper.add(blobId01);
    helper.add(blobId02);
    helper.add(blobId03);
    helper.add(blobId04);

    helper.delete(blobId03);
    helper.add(blobId05);

    helper.add(blobId06);
    helper.add(blobId07);

    helper.delete(blobId02);
    helper.delete(blobId06);

    helper.add(blobId08);
    helper.add(blobId09);

    helper.delete(blobId01);
    helper.add(blobId10);

    helper.undelete(blobId03);
    helper.undelete(blobId01);

    helper.delete(blobId10);
    helper.delete(blobId08);
    // Let enough time to pass so that the above records become eligible for hard deletes.
    time.sleep(TimeUnit.DAYS.toMillis(2));
    return keys;
  }

  private void checkRecordHardDeleted(List<MockId> allKeys, Set<StoreKey> deletedKeys)
      throws IOException, StoreException {
    byte[] zeros = new byte[100];
    Arrays.fill(zeros, (byte) 0);
    for (StoreKey key : allKeys) {
      ByteBuffer recordContent = helper.readRecord(key);
      byte[] obtained = new byte[100];
      recordContent.position(100);
      recordContent.get(obtained, 0, 100);
      if (deletedKeys.contains(key)) {
        assertArrayEquals("HardDeleted Put Record should be zeroed out", obtained, zeros);
      } else {
        assertFalse("HardDeleted Put Record should not be zeroed out", Arrays.equals(zeros, obtained));
      }
    }
  }

  private class MockDiskIOScheduler extends DiskIOScheduler {
    private RuntimeException e;

    /**
     * Create a {@link DiskIOScheduler}.
     */
    MockDiskIOScheduler() {
      super(null);
    }

    @Override
    public long getSlice(String jobType, String jobId, long usedSinceLastCall) {
      if (e != null) {
        throw e;
      }
      return super.getSlice(jobType, jobId, usedSinceLastCall);
    }

    public void setException(Exception e) {
      this.e = e == null ? null : new RuntimeException(e);
    }
  }

  private class MockIndex extends PersistentIndex {

    MockIndex(String datadir, ScheduledExecutorService scheduler, Log log, StoreConfig config, StoreKeyFactory factory,
        MessageStoreHardDelete messageStoreHardDelete, Time time, UUID incarnationId) throws StoreException {
      super(datadir, datadir, scheduler, log, config, factory, new DummyMessageStoreRecovery(), messageStoreHardDelete,
          new DiskIOScheduler(null), new StoreMetrics(new MetricRegistry()), time, new UUID(1, 1), incarnationId);
    }

    MockIndex(String datadir, ScheduledExecutorService scheduler, Log log, StoreConfig config, StoreKeyFactory factory,
        MessageStoreHardDelete messageStoreHardDelete, DiskIOScheduler diskIoScheduler, Time time, UUID incarnationId)
        throws StoreException {
      super(datadir, datadir, scheduler, log, config, factory, new DummyMessageStoreRecovery(), messageStoreHardDelete,
          diskIoScheduler, new StoreMetrics(new MetricRegistry()), time, new UUID(1, 1), incarnationId);
    }

    /**
     * Always returns a {@link FindTokenType#Uninitialized} token.
     * @param token the {@link StoreFindToken} to revalidate.
     * @return a {@link FindTokenType#Uninitialized} token.
     */
    @Override
    FindToken revalidateFindToken(FindToken token) {
      return new StoreFindToken();
    }

    long getHardDeleteProgress() {
      return index.hardDeleter.getProgress();
    }

    void setHardDeleteRunningStatus(boolean status) {
      hardDeleter.enabled.set(status);
    }

    boolean hardDelete() throws StoreException {
      return hardDeleter.hardDelete();
    }

    void persistAndAdvanceStartTokenSafeToPersist() {
      hardDeleter.preLogFlush();
      // no flushing to do.
      hardDeleter.postLogFlush();
    }

    void pruneHardDeleteRecoveryRange() {
      hardDeleter.pruneHardDeleteRecoveryRange();
    }

    void performHardDeleteRecovery() throws StoreException {
      hardDeleter.performRecovery();
    }

    void resetHardDeleterTokens() throws InterruptedException, IOException, StoreException {
      hardDeleter.pause();
      hardDeleter.resume();
    }
  }
}

