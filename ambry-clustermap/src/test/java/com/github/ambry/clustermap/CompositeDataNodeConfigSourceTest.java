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

package com.github.ambry.clustermap;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.TestUtils;
import java.util.Collections;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


public class CompositeDataNodeConfigSourceTest extends DataNodeConfigSourceTestBase {
  DataNodeConfigSourceMetrics metrics = new DataNodeConfigSourceMetrics(new MetricRegistry());
  DataNodeConfigSource primarySource = mock(DataNodeConfigSource.class);
  DataNodeConfigSource secondarySource = mock(DataNodeConfigSource.class);
  MockTime time = new MockTime();
  CompositeDataNodeConfigSource compositeSource =
      new CompositeDataNodeConfigSource(primarySource, secondarySource, time, metrics);

  /**
   * Test functionality related to detecting inconsistencies between two listeners
   */
  @Test
  public void testListener() throws Exception {
    time.allowSleepCalls(0);
    DataNodeConfigChangeListener listener = mock(DataNodeConfigChangeListener.class);
    compositeSource.addDataNodeConfigChangeListener(listener);
    DataNodeConfigChangeListener primaryListener = captureRegisteredListener(primarySource);
    DataNodeConfigChangeListener secondaryListener = captureRegisteredListener(secondarySource);
    // create an inconsistency (this should not make the metric fire until 3*5 mock seconds have passed)
    DataNodeConfig configOne = createConfig(1, 1);
    primaryListener.onDataNodeConfigChange(Collections.singleton(configOne));
    verify(listener).onDataNodeConfigChange(Collections.singleton(configOne));
    time.allowSleepCalls(1);
    assertTrue("Expected 0 inconsistencies so far",
        TestUtils.checkAndSleep(0L, metrics.listenerInconsistencyCount::getCount, 500));
    assertTrue("Expected 1 transient inconsistency so far",
        TestUtils.checkAndSleep(1L, metrics.listenerTransientInconsistencyCount::getCount, 500));
    time.allowSleepCalls(2);
    assertTrue("Expected 1 inconsistency so far",
        TestUtils.checkAndSleep(1L, metrics.listenerInconsistencyCount::getCount, 500));
    assertTrue("Expected 2 transient inconsistencies so far",
        TestUtils.checkAndSleep(2L, metrics.listenerTransientInconsistencyCount::getCount, 500));

    // fix the inconsistency
    secondaryListener.onDataNodeConfigChange(Collections.singleton(configOne));
    time.allowSleepCalls(1);
    assertEquals("Expected no more inconsistencies", 1, metrics.listenerInconsistencyCount.getCount());
    assertEquals("Expected no more transient inconsistencies", 2,
        metrics.listenerTransientInconsistencyCount.getCount());

    // add a second listener, this should only be attached to the primary
    DataNodeConfigChangeListener newListener = mock(DataNodeConfigChangeListener.class);
    compositeSource.addDataNodeConfigChangeListener(newListener);
    verify(primarySource).addDataNodeConfigChangeListener(newListener);
    verify(secondarySource, never()).addDataNodeConfigChangeListener(newListener);
  }

  /**
   * Test composite set with both sources returning the same result.
   */
  @Test
  public void testSetBothSuccess() {
    when(primarySource.set(any())).thenReturn(true);
    when(secondarySource.set(any())).thenReturn(true);
    DataNodeConfig config = createConfig(1, 1);
    assertTrue("Expected success response", compositeSource.set(config));
    verify(primarySource).set(config);
    verify(secondarySource).set(config);
  }

  /**
   * Test composite set when one source returns a different result.
   */
  @Test
  public void testSetResultsDifferent() {
    when(primarySource.set(any())).thenReturn(true);
    when(secondarySource.set(any())).thenReturn(false);
    DataNodeConfig config = createConfig(1, 1);
    assertTrue("Expected response from primary", compositeSource.set(config));
    verify(primarySource).set(config);
    verify(secondarySource).set(config);
    assertEquals("Expected one inconsistency", 1, metrics.setSwallowedErrorCount.getCount());
  }

  /**
   * Test exceptions thrown in the composite set method.
   */
  @Test
  public void testSetException() throws Exception {
    // exception in primary should be thrown
    when(primarySource.set(any())).thenThrow(new RuntimeException());
    DataNodeConfig config = createConfig(1, 1);
    TestUtils.assertException(RuntimeException.class, () -> compositeSource.set(config), null);
    verify(primarySource).set(config);
    verify(secondarySource, never()).set(any());

    // exception in secondary should be swallowed
    reset(primarySource, secondarySource);
    when(primarySource.set(any())).thenReturn(true);
    when(secondarySource.set(any())).thenThrow(new RuntimeException());
    assertTrue("Expected response from primary", compositeSource.set(config));
    verify(primarySource).set(config);
    verify(secondarySource).set(config);
    assertEquals("Expected one inconsistency", 1, metrics.setSwallowedErrorCount.getCount());
  }

  /**
   * Test composite get with both sources returning the same result.
   */
  @Test
  public void testGetBothSuccess() {
    DataNodeConfig config = createConfig(1, 1);
    when(primarySource.get(any())).thenReturn(config);
    when(secondarySource.get(any())).thenReturn(config);
    assertEquals("Unexpected response", config, compositeSource.get(config.getInstanceName()));
    verify(primarySource).get(config.getInstanceName());
    verify(secondarySource).get(config.getInstanceName());
  }

  /**
   * Test composite get when one source returns a different result.
   */
  @Test
  public void testGetResultsDifferent() {
    DataNodeConfig config = createConfig(1, 1);
    when(primarySource.get(any())).thenReturn(config);
    when(secondarySource.get(any())).thenReturn(createConfig(1, 1));
    assertEquals("Expected response from primary", config, compositeSource.get(config.getInstanceName()));
    verify(primarySource).get(config.getInstanceName());
    verify(secondarySource).get(config.getInstanceName());
    assertEquals("Expected one inconsistency", 1, metrics.getSwallowedErrorCount.getCount());
  }

  /**
   * Test exceptions thrown in the composite get method.
   */
  @Test
  public void testGetException() throws Exception {
    // exception in primary should be thrown
    when(primarySource.get(any())).thenThrow(new RuntimeException());
    DataNodeConfig config = createConfig(1, 1);
    TestUtils.assertException(RuntimeException.class, () -> compositeSource.get(config.getInstanceName()), null);
    verify(primarySource).get(config.getInstanceName());
    verify(secondarySource, never()).get(any());

    // exception in secondary should be swallowed
    reset(primarySource, secondarySource);
    when(primarySource.get(any())).thenReturn(config);
    when(secondarySource.get(any())).thenThrow(new RuntimeException());
    assertEquals("Expected response from primary", config, compositeSource.get(config.getInstanceName()));
    verify(primarySource).get(config.getInstanceName());
    verify(secondarySource).get(config.getInstanceName());
    assertEquals("Expected one inconsistency", 1, metrics.getSwallowedErrorCount.getCount());
  }

  /**
   * @param source the mock source to get the registered listener from
   * @return the {@link DataNodeConfigChangeListener} captured.
   */
  private static DataNodeConfigChangeListener captureRegisteredListener(DataNodeConfigSource source) throws Exception {
    ArgumentCaptor<DataNodeConfigChangeListener> listenerCaptor =
        ArgumentCaptor.forClass(DataNodeConfigChangeListener.class);
    verify(source).addDataNodeConfigChangeListener(listenerCaptor.capture());
    return listenerCaptor.getValue();
  }
}
