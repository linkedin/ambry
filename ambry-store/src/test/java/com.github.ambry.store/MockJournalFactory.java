package com.github.ambry.store;

public class MockJournalFactory implements JournalFactory {
  @Override
  public Journal getJournal(String dataDir, int maxEntriesToJournal, int maxEntriesToReturn) {
    return new MockJournal(dataDir, maxEntriesToJournal, maxEntriesToReturn);
  }
}
