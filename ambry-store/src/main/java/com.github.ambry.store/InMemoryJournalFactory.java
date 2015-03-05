package com.github.ambry.store;

public class InMemoryJournalFactory implements JournalFactory {

  @Override
  public Journal getJournal(String dataDir, int maxEntriesToJournal, int maxEntriesToReturn) {
    return new InMemoryJournal(dataDir, maxEntriesToJournal, maxEntriesToReturn);
  }
}
