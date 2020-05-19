/**
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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

/**
 * This is a dedicated exception for {@link StoreErrorCodes#ID_Undeleted}. When {@link Store} throws an Exception
 * for {@link StoreErrorCodes#ID_Undeleted}, it's better that it throws an {@link IdUndeletedStoreException}.
 */
public class IdUndeletedStoreException extends StoreException {
  private final short lifeVersion;

  /**
   * Constructor to create a {@link IdUndeletedStoreException}.
   * @param message The error message.
   * @param lifeVersion The lifeVersion.
   */
  public IdUndeletedStoreException(String message, short lifeVersion) {
    super(message, StoreErrorCodes.ID_Undeleted);
    this.lifeVersion = lifeVersion;
  }

  /**
   * @return The lifeVersion for the current undelete record.
   */
  public short getLifeVersion() {
    return lifeVersion;
  }
}
