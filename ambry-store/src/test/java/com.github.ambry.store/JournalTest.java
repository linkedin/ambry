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

import java.util.List;
import org.junit.Assert;
import org.junit.Test;


public class JournalTest {

  @Test
  public void testJournalOperation() {
    Journal journal = new Journal("test", 10, 5);
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
    List<JournalEntry> entries = journal.getEntriesSince(0, true);
    Assert.assertEquals(entries.get(0).getOffset(), 0);
    Assert.assertEquals(entries.get(0).getKey(), new MockId("id1"));
    Assert.assertEquals(entries.size(), 5);
    Assert.assertEquals(entries.get(4).getOffset(), 4000);
    Assert.assertEquals(entries.get(4).getKey(), new MockId("id5"));
    entries = journal.getEntriesSince(5000, false);
    Assert.assertEquals(entries.get(0).getOffset(), 6000);
    Assert.assertEquals(entries.get(0).getKey(), new MockId("id7"));
    Assert.assertEquals(entries.get(3).getOffset(), 9000);
    Assert.assertEquals(entries.get(3).getKey(), new MockId("id10"));
    Assert.assertEquals(entries.size(), 4);
    entries = journal.getEntriesSince(7000, false);
    Assert.assertEquals(entries.get(0).getOffset(), 8000);
    Assert.assertEquals(entries.get(0).getKey(), new MockId("id9"));
    Assert.assertEquals(entries.get(1).getOffset(), 9000);
    Assert.assertEquals(entries.get(1).getKey(), new MockId("id10"));
    Assert.assertEquals(entries.size(), 2);
    journal.addEntry(10000, new MockId("id11"));
    entries = journal.getEntriesSince(0, true);
    Assert.assertNull(entries);
    entries = journal.getEntriesSince(1000, false);
    Assert.assertEquals(entries.get(0).getOffset(), 2000);
    Assert.assertEquals(entries.get(0).getKey(), new MockId("id3"));
    Assert.assertEquals(entries.size(), 5);
    Assert.assertEquals(entries.get(4).getOffset(), 6000);
    Assert.assertEquals(entries.get(4).getKey(), new MockId("id7"));
  }
}
