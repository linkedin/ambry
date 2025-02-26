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

/**
 * The error codes that the store returns
 */
//@formatter:off
public enum StoreErrorCodes {
  IDNotFound,
  TTLExpired,
  IDDeleted,
  IOError,
  InitializationError,
  AlreadyExist,
  StoreNotStarted,
  StoreAlreadyStarted,
  StoreShuttingDown,
  IllegalIndexOperation,
  IllegalIndexState,
  IndexCreationFailure,
  IndexVersionError,
  AuthorizationFailure,
  UnknownError,
  AlreadyUpdated,
  UpdateNotAllowed,
  FileNotFound,
  ChannelClosed,
  LifeVersionConflict,
  IDNotDeleted,
  IDUndeleted,
  IDDeletedPermanently,
  IDPurged,
  LogFileFormatError,
  IndexFileFormatError,
  LogEndOffsetError,
  IndexRecoveryError
}
//@formatter:on
