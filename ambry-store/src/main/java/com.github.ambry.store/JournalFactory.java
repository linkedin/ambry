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

