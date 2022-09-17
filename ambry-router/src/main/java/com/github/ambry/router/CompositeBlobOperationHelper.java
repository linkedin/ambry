/*
 * Copyright 2021 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.protocol.GetOption;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;


/**
 * A helper class that carries information for ttl update and undelete operation on a potential composite blob.
 */
public class CompositeBlobOperationHelper {
  private final String opName;
  private final GetOption getOption;
  private final NonBlockingRouterMetrics.AgeAtAccessMetrics metrics;
  private final BiConsumer<String, List<String>> doOperation;
  private final Function<BlobInfo, Boolean> alreadyUpdated;
  private final Consumer<RouterException> completeOperationAtException;

  /**
   * Constructor of this helper object.
   * @param opName The name of this operation.
   * @param getOption The {@link GetOption} while fetching the blob ids of a composite blob.
   * @param metrics The {@link NonBlockingRouterMetrics.AgeAtAccessMetrics} to use while fetching blob ids of a composite
   *                blob with {@link GetManager}.
   * @param doOperation The function to call to submit the metadata blob id and a list of chunk ids in String to
   *                    corresponding manager after fetching blob ids.
   * @param alreadyUpdated The function to call to check if the operation is already done.
   * @param completeOperationAtException The function to call when there is exception when calling {@link  OperationController#getBlob}.
   */
  CompositeBlobOperationHelper(String opName, GetOption getOption, NonBlockingRouterMetrics.AgeAtAccessMetrics metrics,
      BiConsumer<String, List<String>> doOperation, Function<BlobInfo, Boolean> alreadyUpdated,
      Consumer<RouterException> completeOperationAtException) {
    this.opName = opName;
    this.getOption = getOption;
    this.metrics = metrics;
    this.doOperation = doOperation;
    this.alreadyUpdated = alreadyUpdated;
    this.completeOperationAtException = completeOperationAtException;
  }

  public String getOpName() {
    return opName;
  }

  public GetOption getGetOption() {
    return getOption;
  }

  public NonBlockingRouterMetrics.AgeAtAccessMetrics getMetrics() {
    return metrics;
  }

  public BiConsumer<String, List<String>> getDoOperation() {
    return doOperation;
  }

  public Function<BlobInfo, Boolean> getAlreadyUpdated() {
    return alreadyUpdated;
  }

  public Consumer<RouterException> getCompleteOperationAtException() {
    return completeOperationAtException;
  }
}
