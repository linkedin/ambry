/*
 * Copyright 2024 LinkedIn Corp. All rights reserved.
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
 *
 */
package com.github.ambry.commons;

import com.github.ambry.rest.RestServiceException;
import com.github.ambry.utils.ThrowingFunction;
import org.junit.Assert;
import org.junit.Test;


/**
 * Tests for {@link CallbackUtils}.
 */
public class CallbackUtilsTest {
  @Test
  public void chainCallbackWithThrowableConsumerTest() {
    String theResult = "result";
    Exception theException = new Exception();
    Error theError = new Error();

    // Test with a successful action, result should be passed to the callback
    CallbackUtils.chainCallback((result, exception) -> {
      Assert.assertSame(theResult, result);
      Assert.assertNull(exception);
    }, (result) -> {
      // Do nothing
    }).onCompletion(theResult, null);

    // Test with preAction throwing an exception, original exception should be passed to the callback
    CallbackUtils.chainCallback((result, exception) -> {
      Assert.assertNull(result);
      Assert.assertSame(theException, exception);
    }, (ThrowingFunction<Object, Object>) (result) -> {
      throw theException;
    }).onCompletion(theResult, null);

    // Test with preAction throwing an error, original exception should be wrapped in a RestServiceException
    // and passed to the callback
    CallbackUtils.chainCallback((result, exception) -> {
      Assert.assertNull(result);
      Assert.assertTrue(exception instanceof RestServiceException);
      Assert.assertSame(exception.getCause(), theError);
    }, (ThrowingFunction<Object, Object>) (result) -> {
      throw theError;
    }).onCompletion(theResult, null);
  }

  @Test
  public void chainCallbackWithThrowableFunctionTest() {
    String result1 = "result1";
    String result2 = "result2";

    // Test with a successful action, returned result from preAction should be passed to the callback
    CallbackUtils.chainCallback((result, exception) -> {
      Assert.assertNotSame(result1, result);
      Assert.assertSame(result2, result);
      Assert.assertNull(exception);
    }, (result) -> {
      return result2;
    }).onCompletion(result1, null);
  }
}