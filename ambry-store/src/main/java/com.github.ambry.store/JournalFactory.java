package com.github.ambry.store;

import java.io.IOException;


/**
 * Factory to create the journal
 */
public interface JournalFactory {

  /**
   * Get the journal
   * @return The journal
   */
  public Journal getJournal(String dataDir, int maxEntriesToJournal, int maxEntriesToReturn);
}

