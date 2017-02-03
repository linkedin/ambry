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
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


/**
 * Tests for {@link HardDeleter}.
 */
public class HardDeleterTest {

  class HardDeleteTestHelper implements MessageStoreHardDelete {
    private long nextOffset;
    private long sizeOfEntry;
    private MockIndex index;
    private Log log;
    private String logSegmentName;
    HashMap<Long, MessageInfo> offsetMap;

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

    void add(MockId id) throws IOException, StoreException {
      Offset offset = new Offset(logSegmentName, nextOffset);
      index.addToIndex(new IndexEntry(id, new IndexValue(sizeOfEntry, offset, (byte) 0, 12345)),
          new FileSpan(offset, new Offset(logSegmentName, nextOffset + sizeOfEntry)));
      ByteBuffer byteBuffer = ByteBuffer.allocate((int) sizeOfEntry);
      log.appendFrom(byteBuffer);
      offsetMap.put(nextOffset, new MessageInfo(id, sizeOfEntry));
      nextOffset += sizeOfEntry;
    }

    void delete(MockId id) throws IOException, StoreException {
      Offset offset = new Offset(logSegmentName, nextOffset);
      index.markAsDeleted(id, new FileSpan(offset, new Offset(logSegmentName, nextOffset + sizeOfEntry)));
      ByteBuffer byteBuffer = ByteBuffer.allocate((int) sizeOfEntry);
      log.appendFrom(byteBuffer);
      nextOffset += sizeOfEntry;
    }

    @Override
    public Iterator<HardDeleteInfo> getHardDeleteMessages(MessageReadSet readSet, StoreKeyFactory factory,
        List<byte[]> recoveryInfoList) {
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
          --count;
          ByteBuffer buf = ByteBuffer.allocate((int) sizeOfEntry);
          byte[] recoveryInfo = new byte[100];
          Arrays.fill(recoveryInfo, (byte) 0);
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

    @Override
    public MessageInfo getMessageInfo(Read read, long offset, StoreKeyFactory factory) {
      return offsetMap.get(offset);
    }
  }

  private MockIndex index = null;
  private HardDeleteTestHelper helper = null;
  private MockTime time = null;
  private ScheduledExecutorService scheduler;
  private Log log;

  @Before
  public void setup() throws Exception {
    File rootDirectory = StoreTestUtils.createTempDirectory("ambry");
    File indexFile = new File(rootDirectory.getAbsolutePath());
    for (File c : indexFile.listFiles()) {
      c.delete();
    }
    scheduler = Utils.newScheduler(1, false);
    log = new Log(rootDirectory.getAbsolutePath(), 10000, 10000,
        new StoreMetrics(rootDirectory.getAbsolutePath(), new MetricRegistry()));
    Properties props = new Properties();
    // the test will set the tokens, so disable the index persistor.
    props.setProperty("store.data.flush.interval.seconds", "3600");
    props.setProperty("store.deleted.message.retention.days", "1");
    props.setProperty("store.index.max.number.of.inmem.elements", "2");
    // the following determines the number of entries that will be fetched at most. We need this to test the
    // case where the endToken does not reach the journal.
    props.setProperty("store.hard.delete.bytes.per.sec", "40");
    StoreConfig config = new StoreConfig(new VerifiableProperties(props));
    StoreKeyFactory factory = Utils.getObj("com.github.ambry.store.MockIdFactory");
    time = new MockTime(SystemTime.getInstance().milliseconds());

    helper = new HardDeleteTestHelper(0, 200);
    index = new MockIndex(rootDirectory.getAbsolutePath(), scheduler, log, config, factory, helper, time,
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
    index.close();
    log.close();
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
      helper.add(blobId10);

      helper.delete(blobId10);
      helper.delete(blobId01);
      helper.delete(blobId08);

      // Let enough time to pass so that the above records become eligible for hard deletes.
      time.currentMilliseconds = time.currentMilliseconds + 2 * Time.SecsPerDay * Time.MsPerSec;

      // The first * shows where startTokenSafeToPersist is
      // The second * shows where startToken/endToken are before the operations.
      // Note since we are only depicting values before and after hardDelete() is done, startToken and endToken
      // will be the same.

      // indexes: **[1 2] [3 4] [3d 5] [6 7] [2d 6d] [8 9] [1d 10d] [8]
      // journal:                                       [10 10d 1d 8]
      boolean tokenMovedForward = index.hardDelete();
      Assert.assertTrue(tokenMovedForward);
      // startToken = endToken = 2.

      // call into the log flush hooks so that startTokenSafeToPersist = startToken = 2.
      index.persistAndAdvanceStartTokenSafeToPersist();

      // indexes: [1 2**] [3 4] [3d 5] [6 7] [2d 6d] [8 9] [1d 10d] [8]
      // journal:                                       [10 10d 1d 8]
      tokenMovedForward = index.hardDelete();
      Assert.assertTrue(tokenMovedForward);
      // startToken = endToken = 4.

      // indexes: [1 2*] [3 4*] [3d 5] [6 7] [2d 6d] [8 9] [1d 10d] [8]
      // journal:                                       [10 10d 1d 8]
      tokenMovedForward = index.hardDelete();
      Assert.assertTrue(tokenMovedForward);
      // startToken = 5, endToken = 5, startTokenSafeToPersist = 2

      // indexes: [1 2*] [3 4] [3d 5*] [6 7] [2d 6d] [8 9] [1d 10d] [8]
      // journal:                                       [10 10d 1d 8]
      index.persistAndAdvanceStartTokenSafeToPersist();
      tokenMovedForward = index.hardDelete();
      Assert.assertTrue(tokenMovedForward);
      // startToken = 7, endToken = 7, starttokenSafeToPersist = 5
      // 3d just got pruned.

      // indexes: [1 2] [3 4] [3d 5*] [6 7*] [2d 6d] [8 9] [1d 10d] [8]
      // journal:                                       [10 10d 1d 8]
      tokenMovedForward = index.hardDelete();
      Assert.assertTrue(tokenMovedForward);

      // indexes: [1 2] [3 4] [3d 5*] [6 7] [2d 6d*] [8 9] [1d 10d] [8]
      // journal:                                       [10 10d 1d 8]
      tokenMovedForward = index.hardDelete();
      Assert.assertTrue(tokenMovedForward);

      // indexes: [1 2] [3 4] [3d 5*] [6 7] [2d 6d] [8 9*] [1d 10d] [8]
      // journal:                                       [10 10d 1d 8]
      index.persistAndAdvanceStartTokenSafeToPersist();
      tokenMovedForward = index.hardDelete();
      Assert.assertTrue(tokenMovedForward);

      // indexes: [1 2] [3 4] [3d 5] [6 7] [2d 6d] [8 9*] [1d 10d] [8]
      // journal:                                       [10 10d* 1d 8]

      tokenMovedForward = index.hardDelete();
      Assert.assertTrue(tokenMovedForward);

      // indexes: [1 2] [3 4] [3d 5] [6 7] [2d 6d] [8 9*] [1d 10d] [8]
      // journal:                                       [10 10d 1d 8*]
      // All caught up, so token should not have moved forward.
      tokenMovedForward = index.hardDelete();
      Assert.assertFalse(tokenMovedForward);

      index.persistAndAdvanceStartTokenSafeToPersist();

      // directly prune the recovery range completely (which should happen since we flushed till the endToken).
      index.pruneHardDeleteRecoveryRange(); //

      // Test recovery - this tests reading from the persisted token, filling up the hard delete recovery range and
      // then actually redoing the hard deletes on the range.
      index.performHardDeleteRecovery();

      index.close();
    } catch (Exception e) {
      e.printStackTrace();
      org.junit.Assert.assertEquals(false, true);
    }
  }
}
