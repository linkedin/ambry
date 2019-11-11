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

import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;


public class JournalTest {

  @Test
  public void testJournalOperation() {
    long pos = Utils.getRandomLong(TestUtils.RANDOM, 1000);
    long gen = Utils.getRandomLong(TestUtils.RANDOM, 1000);
    String firstLogSegmentName = LogSegmentNameHelper.getName(pos, gen);
    String secondLogSegmentName = LogSegmentNameHelper.getNextPositionName(firstLogSegmentName);
    Journal journal = new Journal("test", 10, 5);
    // maintain a queue to keep track of entries in journal for verification purpose
    List<JournalEntry> journalEntries = new LinkedList<>();
    Assert.assertNull("First offset should be null", journal.getFirstOffset());
    Assert.assertNull("Last offset should be null", journal.getLastOffset());
    Assert.assertNull("Should not be able to get entries because there are none",
        journal.getEntriesSince(new Offset(firstLogSegmentName, 0), true));
    Assert.assertTrue("Get all entries should return empty list", journal.getAllEntries().isEmpty());
    addEntryAndVerify(journal, new Offset(firstLogSegmentName, 0), new MockId("id1"), journalEntries);
    Assert.assertEquals("Did not get expected key at offset", new MockId("id1"),
        journal.getKeyAtOffset(new Offset(firstLogSegmentName, 0)));
    addEntryAndVerify(journal, new Offset(firstLogSegmentName, 1000), new MockId("id2"), journalEntries);
    addEntryAndVerify(journal, new Offset(firstLogSegmentName, 2000), new MockId("id3"), journalEntries);
    addEntryAndVerify(journal, new Offset(firstLogSegmentName, 3000), new MockId("id4"), journalEntries);
    addEntryAndVerify(journal, new Offset(firstLogSegmentName, 4000), new MockId("id5"), journalEntries);
    addEntryAndVerify(journal, new Offset(secondLogSegmentName, 0), new MockId("id6"), journalEntries);
    addEntryAndVerify(journal, new Offset(secondLogSegmentName, 1000), new MockId("id7"), journalEntries);
    addEntryAndVerify(journal, new Offset(secondLogSegmentName, 2000), new MockId("id8"), journalEntries);
    addEntryAndVerify(journal, new Offset(secondLogSegmentName, 3000), new MockId("id9"), journalEntries);
    addEntryAndVerify(journal, new Offset(secondLogSegmentName, 4000), new MockId("id10"), journalEntries);
    Assert.assertEquals("First offset not as expected", new Offset(firstLogSegmentName, 0), journal.getFirstOffset());
    Assert.assertEquals("Last offset not as expected", new Offset(secondLogSegmentName, 4000), journal.getLastOffset());
    Assert.assertEquals("Current entries in journal not expected", journalEntries, journal.getAllEntries());
    List<JournalEntry> entries = journal.getEntriesSince(new Offset(firstLogSegmentName, 0), true);
    Assert.assertEquals(entries.get(0).getOffset(), new Offset(firstLogSegmentName, 0));
    Assert.assertEquals(entries.get(0).getKey(), new MockId("id1"));
    Assert.assertEquals(entries.size(), 5);
    Assert.assertEquals(entries.get(4).getOffset(), new Offset(firstLogSegmentName, 4000));
    Assert.assertEquals(entries.get(4).getKey(), new MockId("id5"));
    entries = journal.getEntriesSince(new Offset(secondLogSegmentName, 0), false);
    Assert.assertEquals(entries.get(0).getOffset(), new Offset(secondLogSegmentName, 1000));
    Assert.assertEquals(entries.get(0).getKey(), new MockId("id7"));
    Assert.assertEquals(entries.get(3).getOffset(), new Offset(secondLogSegmentName, 4000));
    Assert.assertEquals(entries.get(3).getKey(), new MockId("id10"));
    Assert.assertEquals(entries.size(), 4);
    entries = journal.getEntriesSince(new Offset(secondLogSegmentName, 2000), false);
    Assert.assertEquals(entries.get(0).getOffset(), new Offset(secondLogSegmentName, 3000));
    Assert.assertEquals(entries.get(0).getKey(), new MockId("id9"));
    Assert.assertEquals(entries.get(1).getOffset(), new Offset(secondLogSegmentName, 4000));
    Assert.assertEquals(entries.get(1).getKey(), new MockId("id10"));
    Assert.assertEquals(entries.size(), 2);
    entries = journal.getEntriesSince(new Offset(firstLogSegmentName, 2000), false);
    Assert.assertEquals(entries.get(0).getOffset(), new Offset(firstLogSegmentName, 3000));
    Assert.assertEquals(entries.get(0).getKey(), new MockId("id4"));
    Assert.assertEquals(entries.get(4).getOffset(), new Offset(secondLogSegmentName, 2000));
    Assert.assertEquals(entries.get(4).getKey(), new MockId("id8"));
    Assert.assertEquals(entries.size(), 5);
    Assert.assertNull("Should not be able to get entries because offset does not exist",
        journal.getEntriesSince(new Offset(firstLogSegmentName, 1), true));
    addEntryAndVerify(journal, new Offset(secondLogSegmentName, 5000), new MockId("id11"), journalEntries);
    Assert.assertEquals("Current entries in journal not expected", journalEntries, journal.getAllEntries());
    Assert.assertEquals("Number of entries in journal not expected", journal.getMaxEntriesToJournal(),
        journal.getAllEntries().size());
    Assert.assertEquals("First offset not as expected", new Offset(firstLogSegmentName, 1000),
        journal.getFirstOffset());
    Assert.assertEquals("Last offset not as expected", new Offset(secondLogSegmentName, 5000), journal.getLastOffset());
    Assert.assertNull("Journal should have no entries for the first added offset",
        journal.getKeyAtOffset(new Offset(firstLogSegmentName, 0)));
    Assert.assertNull("Journal should have no entries for the offset after the last one",
        journal.getKeyAtOffset(new Offset(firstLogSegmentName, 5001)));
    Assert.assertNull("Journal should have no entries for offsets that weren't added",
        journal.getKeyAtOffset(new Offset(firstLogSegmentName, 1001)));
    entries = journal.getEntriesSince(new Offset(firstLogSegmentName, 0), true);
    Assert.assertNull(entries);
    entries = journal.getEntriesSince(new Offset(firstLogSegmentName, 1000), false);
    Assert.assertEquals(entries.get(0).getOffset(), new Offset(firstLogSegmentName, 2000));
    Assert.assertEquals(entries.get(0).getKey(), new MockId("id3"));
    Assert.assertEquals(entries.size(), 5);
    Assert.assertEquals(entries.get(4).getOffset(), new Offset(secondLogSegmentName, 1000));
    Assert.assertEquals(entries.get(4).getKey(), new MockId("id7"));
  }

  /**
   * Tests the {@link Journal}'s bootstrap mode by performing the following operations:
   * 1. Add entries before going into bootstrap mode and the entries in the journal should respect the max constraint
   * 2. Start bootstrap mode and add more entries. Verify the entries in the journal can exceed the max constraint
   * 3. Finish bootstrap mode and verify the entries in the journal are respecting the max constraint again
   */
  @Test
  public void testJournalBootstrapMode() {
    long pos = Utils.getRandomLong(TestUtils.RANDOM, 1000);
    long gen = Utils.getRandomLong(TestUtils.RANDOM, 1000);
    String logSegmentName = LogSegmentNameHelper.getName(pos, gen);
    Offset[] offsets = new Offset[4];
    MockId[] keys = new MockId[4];
    for (int i = 0; i < 4; i++) {
      offsets[i] = new Offset(logSegmentName, i * 1000);
      keys[i] = new MockId("id" + i);
    }
    List<JournalEntry> journalEntries = new ArrayList<>();
    // Bootstrap mode is off by default and journal entries should respect the max constraint
    Journal journal = new Journal("test", 1, 1);
    addEntryAndVerify(journal, offsets[0], keys[0], journalEntries);
    addEntryAndVerify(journal, offsets[1], keys[1], journalEntries);
    Assert.assertEquals("Unexpected journal size", 1, journal.getCurrentNumberOfEntries());
    Assert.assertEquals("Oldest entry is not being replaced", offsets[1], journal.getFirstOffset());
    Assert.assertEquals("Entries in journal not expected", journalEntries, journal.getAllEntries());
    // Bootstrap mode is turned on and journal entries should be able to exceed the max constraint
    journal.startBootstrap();
    addEntryAndVerify(journal, offsets[2], keys[2], journalEntries);
    Assert.assertEquals("Unexpected journal size", 2, journal.getCurrentNumberOfEntries());
    Assert.assertEquals("Oldest entry should not be replaced", offsets[1], journal.getFirstOffset());
    Assert.assertEquals("Entries in journal not expected", journalEntries, journal.getAllEntries());
    // Bootstrap mode is off and journal entries should respect the max constraint again
    journal.finishBootstrap();
    addEntryAndVerify(journal, offsets[3], keys[3], journalEntries);
    Assert.assertEquals("Unexpected journal size", 2, journal.getCurrentNumberOfEntries());
    Assert.assertEquals("Oldest entry is not being replaced", offsets[2], journal.getFirstOffset());
    Assert.assertEquals("Entries in journal not expected", journalEntries, journal.getAllEntries());
  }

  /**
   * Adds an entry to the journal and verifies some getters
   * @param journal the {@link Journal} to add to
   * @param offset the {@link Offset} to add an entry for
   * @param id the {@link StoreKey}  at {@code offset}
   * @param journalEntries a list of {@link JournalEntry} to track entries in current journal. This is for verification
   *                       purpose.
   */
  private void addEntryAndVerify(Journal journal, Offset offset, MockId id, List<JournalEntry> journalEntries) {
    long crc = Utils.getRandomLong(TestUtils.RANDOM, Long.MAX_VALUE);
    journal.addEntry(offset, id, crc);
    if (journalEntries != null) {
      if (!journal.isInBootstrapMode() && journalEntries.size() >= journal.getMaxEntriesToJournal()) {
        journalEntries.remove(0);
      }
      journalEntries.add(new JournalEntry(offset, id));
    }
    Assert.assertEquals("Unexpected key at offset", id, journal.getKeyAtOffset(offset));
    Assert.assertEquals("Unexpected crc for key", crc, journal.getCrcOfKey(id).longValue());
  }
}
