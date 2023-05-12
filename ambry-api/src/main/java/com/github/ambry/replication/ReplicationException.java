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
package com.github.ambry.replication;

import com.github.ambry.server.ServerErrorCode;


public class ReplicationException extends Exception {
  private static final long serialVersionUID = 1;
  private final ServerErrorCode serverErrorCode;

  public ReplicationException(String message) {
    super(message);
    serverErrorCode = null;
  }

  public ReplicationException(String message, Throwable e) {
    super(message, e);
    serverErrorCode = null;
  }

  public ReplicationException(Throwable e) {
    super(e);
    serverErrorCode = null;
  }

  public ReplicationException(String message, ServerErrorCode errorCode) {
    super(message);
    serverErrorCode = errorCode;
  }

  public ServerErrorCode getServerErrorCode() {
    return serverErrorCode;
  }
}