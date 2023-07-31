/*
 * Copyright 2023 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.named;

import java.sql.Connection;


/**
 * Transaction isolation level. Name of the enum must match to constant names in java {@link Connection} class such as
 * {@link Connection#TRANSACTION_NONE} or {@link Connection#TRANSACTION_READ_UNCOMMITTED} or
 * {@link Connection#TRANSACTION_READ_COMMITTED} or {@link Connection#TRANSACTION_REPEATABLE_READ} or
 * {@link Connection#TRANSACTION_SERIALIZABLE}
 */
public enum TransactionIsolationLevel {
  TRANSACTION_NONE,
  TRANSACTION_READ_UNCOMMITTED,
  TRANSACTION_READ_COMMITTED,
  TRANSACTION_REPEATABLE_READ,
  TRANSACTION_SERIALIZABLE;
}
