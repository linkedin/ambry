package com.github.ambry.store;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class BlobJournalTest {

  @Test
  public void testJournalOperation() {
    BlobJournal journal = new BlobJournal("test", 10, 5);
    journal.addEntry(0, new MockId("id1"));
    journal.addEntry(1000, new MockId("id2"));
    journal.addEntry(2000, new MockId("id3"));
    journal.addEntry(3000, new MockId("id4"));
    journal.addEntry(4000, new MockId("id5"));
    journal.addEntry(5000, new MockId("id6"));
    journal.addEntry(6000, new MockId("id7"));
    journal.addEntry(7000, new MockId("id8"));
    journal.addEntry(8000, new MockId("id9"));
    journal.addEntry(9000, new MockId("id10"));
    List<JournalEntry> entries = journal.getEntriesSince(0);
    Assert.assertEquals(entries.get(0).getOffset(), 0);
    Assert.assertEquals(entries.get(0).getKey(), new MockId("id1"));
    Assert.assertEquals(entries.size(), 5);
    Assert.assertEquals(entries.get(4).getOffset(), 4000);
    Assert.assertEquals(entries.get(4).getKey(), new MockId("id5"));
    entries = journal.getEntriesSince(5000);
    Assert.assertEquals(entries.get(0).getOffset(), 5000);
    Assert.assertEquals(entries.get(0).getKey(), new MockId("id6"));
    Assert.assertEquals(entries.get(4).getOffset(), 9000);
    Assert.assertEquals(entries.get(4).getKey(), new MockId("id10"));
    Assert.assertEquals(entries.size(), 5);
    entries = journal.getEntriesSince(7000);
    Assert.assertEquals(entries.get(0).getOffset(), 7000);
    Assert.assertEquals(entries.get(0).getKey(), new MockId("id8"));
    Assert.assertEquals(entries.get(2).getOffset(), 9000);
    Assert.assertEquals(entries.get(2).getKey(), new MockId("id10"));
    Assert.assertEquals(entries.size(), 3);
    journal.addEntry(10000, new MockId("id11"));
    entries = journal.getEntriesSince(0);
    Assert.assertNull(entries);
    entries = journal.getEntriesSince(1000);
    Assert.assertEquals(entries.get(0).getOffset(), 1000);
    Assert.assertEquals(entries.get(0).getKey(), new MockId("id2"));
    Assert.assertEquals(entries.size(), 5);
    Assert.assertEquals(entries.get(4).getOffset(), 5000);
    Assert.assertEquals(entries.get(4).getKey(), new MockId("id6"));
  }

}
