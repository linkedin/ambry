/*
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
 *
 */

package com.github.ambry.utils;

import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A blocking queue that provides a special timed poll method that allows getting all elements currently present in the
 * queue. It also supports early wakeup of the timed poll using the {@link #wakeup} method.
 * @param <E> the type of element this queue contains
 */
public class BatchBlockingQueue<E> {
  private static final Logger LOGGER = LoggerFactory.getLogger(BatchBlockingQueue.class);
  private final BlockingQueue<E> queue = new LinkedBlockingQueue<>();
  private final E wakeupMarker;

  /**
   * Construct a queue.
   * @param wakeupMarker a marker object that will be used to wake up the queue from a timed poll even if there are no
   *                     other elements in the queue. This object instance cannot be inserted into the queue using the
   *                     {@link #put} method. Objects that are equal to {@code wakeupMarker} but not the same instance
   *                     can be inserted into the queue.
   */
  public BatchBlockingQueue(E wakeupMarker) {
    this.wakeupMarker = Objects.requireNonNull(wakeupMarker);
  }

  /**
   * Put an element into the queue.
   * @param e the element to add. This cannot be equal to the wakeup marker.
   */
  public void put(E e) {
    if (e == wakeupMarker) {
      throw new IllegalArgumentException("Cannot enqueue the wakeup marker using put(): " + e);
    }
    try {
      queue.put(e);
    } catch (InterruptedException ie) {
      LOGGER.warn("Interrupted putting response into queue", ie);
    }
  }

  /**
   * Poll for new items in the queue. If {@link #wakeup()} is called from another thread, or the timeout otherwise
   * expires, this may return an empty list.
   * @param pollTimeoutMs the poll timeout in milliseconds.
   * @return a list of items taken out of the queue.
   */
  public List<E> poll(int pollTimeoutMs) {
    List<E> returnList = new ArrayList<>();
    poll(returnList, pollTimeoutMs);
    return returnList;
  }

  /**
   * Like {@link #poll(int)} but allows the caller to pass in their own list.
   * @param returnList the list to add items taken off the queue to.
   * @param pollTimeoutMs the poll timeout in milliseconds.
   */
  public void poll(List<E> returnList, int pollTimeoutMs) {
    E first = null;
    try {
      first = queue.poll(pollTimeoutMs, TimeUnit.MILLISECONDS);
    } catch (InterruptedException ie) {
      LOGGER.warn("Interrupted polling for responses in queue", ie);
    }
    if (first == null) {
      return;
    }
    // Filter out any wakeup markers that may be in the queue. These are only there to stop a timed poll early.
    FilteredInserter<E> filteredInserter =
        new BatchBlockingQueue.FilteredInserter<>(returnList, e -> e != wakeupMarker);
    filteredInserter.add(first);
    queue.drainTo(filteredInserter);
  }

  /**
   * Called to force a timed poll call to return before the timeout expires.
   */
  public void wakeup() {
    // if sendAndPoll is currently executing a timed poll on the blocking queue, we need to put WAKEUP_MARKER
    // to wake it up before a response comes
    try {
      queue.put(wakeupMarker);
    } catch (InterruptedException ie) {
      LOGGER.warn("Interrupted while waking up queue", ie);
    }
  }

  /**
   * @return the number of elements in the queue.
   */
  public int size() {
    return queue.size();
  }

  /**
   * An implementation of {@link AbstractCollection} to be used to insert items that match a predicate into a
   * collection. This can be useful when methods add items to a {@link Collections}, but
   */
  private static class FilteredInserter<T> extends AbstractCollection<T> {
    private final Collection<T> data;
    private final Predicate<T> shouldAdd;

    /**
     *
     * @param data the backing collection.
     * @param shouldAdd a predicate that returns true if an item should be added to the backing collection.
     */
    FilteredInserter(Collection<T> data, Predicate<T> shouldAdd) {
      this.data = data;
      this.shouldAdd = shouldAdd;
    }

    @Override
    public boolean add(T t) {
      if (shouldAdd.test(t)) {
        return data.add(t);
      }
      return false;
    }

    @Override
    public Iterator<T> iterator() {
      return data.iterator();
    }

    @Override
    public int size() {
      return data.size();
    }
  }
}
