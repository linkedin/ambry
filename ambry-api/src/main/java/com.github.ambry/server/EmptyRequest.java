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
package com.github.ambry.server;

import com.github.ambry.network.NetworkRequest;
import com.github.ambry.utils.SystemTime;
import java.io.InputStream;


/**
 * The request class used to identify the end of the network communication
 */
public class EmptyRequest implements NetworkRequest {
  private final long startTimeInMs;
  private static EmptyRequest ourInstance = new EmptyRequest();

  public static EmptyRequest getInstance() {
    return ourInstance;
  }

  private EmptyRequest() {
    startTimeInMs = SystemTime.getInstance().milliseconds();
  }

  @Override
  public InputStream getInputStream() {
    return null;
  }

  @Override
  public long getStartTimeInMs() {
    return startTimeInMs;
  }
}
