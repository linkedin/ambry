package com.github.ambry.store;

import java.io.IOException;


/**
 * Factory to create the journal
 */
public interface JournalFactory {

  /**
   * Get the journal
   * @param dataDir the directory used for the index corresponding to this journal.
   * @param maxEntriesToJournal the maximum number of entries this journal can hold.
   * @param maxEntriesToReturn the maximum number of entries this journal can return in a getEntries call.
   * @return The journal
   */
  public Journal getJournal(String dataDir, int maxEntriesToJournal, int maxEntriesToReturn);
}

