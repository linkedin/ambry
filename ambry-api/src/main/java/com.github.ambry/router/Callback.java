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
package com.github.ambry.router;

/**
 * A callback interface that the user can implement to allow code to execute when the request is complete. This callback
 * will generally execute in the background I/O thread so it should be fast.
 */
public interface Callback<T> {
  /**
   * The callback method that the user can implement to do asynchronous handling of the response. T defines the type of
   * the result. When T=Void then there is no specific response expected but any errors are reported on the
   * exception object. For all other values of T, one of the two arguments would be null.
   * @param result The result of the request. This would be non null when the request executed successfully
   * @param exception The exception that was reported on execution of the request
   */
  void onCompletion(T result, Exception exception);
}
