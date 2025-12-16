/*
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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

package com.github.ambry.clustermap;

import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.VerifiableProperties;
import java.util.Properties;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Test for {@link HelixFactory}.
 */
public class HelixFactoryTest {

  private static final String CLUSTER_NAME = "test-cluster";
  private static final String INSTANCE_NAME = "localhost_1234";
  private static final String ZK_ADDR = "localhost:2181";

  /**
   * Test ManagerKey equals and hashCode methods.
   */
  @Test
  public void testManagerKeyEqualsAndHashCode() {
    HelixFactory.ManagerKey key1 = new HelixFactory.ManagerKey(CLUSTER_NAME, INSTANCE_NAME, InstanceType.PARTICIPANT, ZK_ADDR);
    HelixFactory.ManagerKey key2 = new HelixFactory.ManagerKey(CLUSTER_NAME, INSTANCE_NAME, InstanceType.PARTICIPANT, ZK_ADDR);
    HelixFactory.ManagerKey key3 = new HelixFactory.ManagerKey("different-cluster", INSTANCE_NAME, InstanceType.PARTICIPANT, ZK_ADDR);
    HelixFactory.ManagerKey key4 = new HelixFactory.ManagerKey(CLUSTER_NAME, "different-instance", InstanceType.PARTICIPANT, ZK_ADDR);
    HelixFactory.ManagerKey key5 = new HelixFactory.ManagerKey(CLUSTER_NAME, INSTANCE_NAME, InstanceType.SPECTATOR, ZK_ADDR);
    HelixFactory.ManagerKey key6 = new HelixFactory.ManagerKey(CLUSTER_NAME, INSTANCE_NAME, InstanceType.PARTICIPANT, "different-zk");

    // Test equals
    assertEquals("Same keys should be equal", key1, key2);
    assertNotEquals("Different cluster should not be equal", key1, key3);
    assertNotEquals("Different instance should not be equal", key1, key4);
    assertNotEquals("Different instance type should not be equal", key1, key5);
    assertNotEquals("Different zk address should not be equal", key1, key6);
    assertNotEquals("Key should not equal null", key1, null);
    assertNotEquals("Key should not equal different class", key1, "string");

    // Test reflexive
    assertEquals("Key should equal itself", key1, key1);

    // Test hashCode consistency
    assertEquals("Equal keys should have same hash code", key1.hashCode(), key2.hashCode());
    assertNotEquals("Different keys should have different hash codes", key1.hashCode(), key3.hashCode());
  }

  /**
   * Test getZKHelixManager with auto-registration disabled.
   */
  @Test
  public void testGetZKHelixManagerWithoutAutoRegistration() {
    Properties props = new Properties();
    props.setProperty("clustermap.cluster.name", CLUSTER_NAME);
    props.setProperty("clustermap.datacenter.name", "DC1");
    props.setProperty("clustermap.host.name", "localhost");
    props.setProperty("clustermap.port", "1234");
    props.setProperty("clustermap.auto.registration.enabled", "false");
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));

    HelixFactory helixFactory = new HelixFactory();
    HelixManager manager1 = helixFactory.getZKHelixManager(CLUSTER_NAME, INSTANCE_NAME, InstanceType.PARTICIPANT, ZK_ADDR, clusterMapConfig);
    HelixManager manager2 = helixFactory.getZKHelixManager(CLUSTER_NAME, INSTANCE_NAME, InstanceType.PARTICIPANT, ZK_ADDR, clusterMapConfig);

    assertNotNull("Manager should not be null", manager1);
    assertSame("Same manager should be returned for same parameters", manager1, manager2);
  }

  /**
   * Test getZKHelixManager with auto-registration enabled.
   */
  @Test
  public void testGetZKHelixManagerWithAutoRegistration() {
    Properties props = new Properties();
    props.setProperty("clustermap.cluster.name", CLUSTER_NAME);
    props.setProperty("clustermap.datacenter.name", "DC1");
    props.setProperty("clustermap.host.name", "localhost");
    props.setProperty("clustermap.port", "1234");
    props.setProperty("clustermap.auto.registration.enabled", "true");
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));

    HelixFactory helixFactory = new HelixFactory();
    HelixManager manager = helixFactory.getZKHelixManager(CLUSTER_NAME, INSTANCE_NAME, InstanceType.PARTICIPANT, ZK_ADDR, clusterMapConfig);

    assertNotNull("Manager should not be null", manager);
  }

  /**
   * Test getZKHelixManager with different instance types.
   */
  @Test
  public void testGetZKHelixManagerWithDifferentInstanceTypes() {
    Properties props = new Properties();
    props.setProperty("clustermap.cluster.name", CLUSTER_NAME);
    props.setProperty("clustermap.datacenter.name", "DC1");
    props.setProperty("clustermap.host.name", "localhost");
    props.setProperty("clustermap.port", "1234");
    props.setProperty("clustermap.auto.registration.enabled", "true");
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));

    HelixFactory helixFactory = new HelixFactory();
    HelixManager participantManager = helixFactory.getZKHelixManager(CLUSTER_NAME, INSTANCE_NAME, InstanceType.PARTICIPANT, ZK_ADDR, clusterMapConfig);
    HelixManager spectatorManager = helixFactory.getZKHelixManager(CLUSTER_NAME, INSTANCE_NAME, InstanceType.SPECTATOR, ZK_ADDR, clusterMapConfig);

    assertNotNull("Participant manager should not be null", participantManager);
    assertNotNull("Spectator manager should not be null", spectatorManager);
    assertNotSame("Different instance types should return different managers", participantManager, spectatorManager);
  }

  /**
   * Test getZKHelixManager caching behavior.
   */
  @Test
  public void testGetZKHelixManagerCaching() {
    Properties props = new Properties();
    props.setProperty("clustermap.cluster.name", CLUSTER_NAME);
    props.setProperty("clustermap.datacenter.name", "DC1");
    props.setProperty("clustermap.host.name", "localhost");
    props.setProperty("clustermap.port", "1234");
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));

    HelixFactory helixFactory = new HelixFactory();
    
    // Same parameters should return cached instance
    HelixManager manager1 = helixFactory.getZKHelixManager(CLUSTER_NAME, INSTANCE_NAME, InstanceType.PARTICIPANT, ZK_ADDR, clusterMapConfig);
    HelixManager manager2 = helixFactory.getZKHelixManager(CLUSTER_NAME, INSTANCE_NAME, InstanceType.PARTICIPANT, ZK_ADDR, clusterMapConfig);
    assertSame("Same parameters should return cached manager", manager1, manager2);

    // Different parameters should return different instances
    HelixManager manager3 = helixFactory.getZKHelixManager("different-cluster", INSTANCE_NAME, InstanceType.PARTICIPANT, ZK_ADDR, clusterMapConfig);
    assertNotSame("Different cluster should return different manager", manager1, manager3);

    HelixManager manager4 = helixFactory.getZKHelixManager(CLUSTER_NAME, "different-instance", InstanceType.PARTICIPANT, ZK_ADDR, clusterMapConfig);
    assertNotSame("Different instance should return different manager", manager1, manager4);

    HelixManager manager5 = helixFactory.getZKHelixManager(CLUSTER_NAME, INSTANCE_NAME, InstanceType.SPECTATOR, ZK_ADDR, clusterMapConfig);
    assertNotSame("Different instance type should return different manager", manager1, manager5);

    HelixManager manager6 = helixFactory.getZKHelixManager(CLUSTER_NAME, INSTANCE_NAME, InstanceType.PARTICIPANT, "different-zk", clusterMapConfig);
    assertNotSame("Different ZK address should return different manager", manager1, manager6);
  }

  /**
   * Test getZkHelixManagerAndConnect method.
   */
  @Test
  public void testGetZkHelixManagerAndConnect() throws Exception {
    Properties props = new Properties();
    props.setProperty("clustermap.cluster.name", CLUSTER_NAME);
    props.setProperty("clustermap.datacenter.name", "DC1");
    props.setProperty("clustermap.host.name", "localhost");
    props.setProperty("clustermap.port", "1234");
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));

    HelixFactory helixFactory = spy(new HelixFactory());
    HelixManager mockManager = mock(HelixManager.class);
    
    // Mock the buildZKHelixManager method to return our mock
    doReturn(mockManager).when(helixFactory).buildZKHelixManager(anyString(), anyString(), any(InstanceType.class), anyString(), any(ClusterMapConfig.class));
    
    // Test when manager is not connected
    when(mockManager.isConnected()).thenReturn(false);
    HelixManager result1 = helixFactory.getZkHelixManagerAndConnect(CLUSTER_NAME, INSTANCE_NAME, InstanceType.PARTICIPANT, ZK_ADDR, clusterMapConfig);
    
    assertSame("Should return the same manager", mockManager, result1);
    verify(mockManager, times(1)).connect();

    // Test when manager is already connected
    when(mockManager.isConnected()).thenReturn(true);
    HelixManager result2 = helixFactory.getZkHelixManagerAndConnect(CLUSTER_NAME, INSTANCE_NAME, InstanceType.PARTICIPANT, ZK_ADDR, clusterMapConfig);
    
    assertSame("Should return the same manager", mockManager, result2);
    // connect() should not be called again
    verify(mockManager, times(1)).connect();
  }

  /**
   * Test getDataNodeConfigSource method.
   */
  @Test
  public void testGetDataNodeConfigSource() {
    Properties props = new Properties();
    props.setProperty("clustermap.cluster.name", CLUSTER_NAME);
    props.setProperty("clustermap.datacenter.name", "DC1");
    props.setProperty("clustermap.host.name", "localhost");
    props.setProperty("clustermap.port", "1234");
    props.setProperty("clustermap.data.node.config.source.type", "PROPERTY_STORE");
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));

    HelixFactory helixFactory = new HelixFactory();
    DataNodeConfigSourceMetrics metrics = mock(DataNodeConfigSourceMetrics.class);
    
    DataNodeConfigSource source1 = helixFactory.getDataNodeConfigSource(clusterMapConfig, ZK_ADDR, metrics);
    DataNodeConfigSource source2 = helixFactory.getDataNodeConfigSource(clusterMapConfig, ZK_ADDR, metrics);
    
    assertNotNull("DataNodeConfigSource should not be null", source1);
    assertSame("Same ZK address should return cached source", source1, source2);
    
    // Different ZK address should return different source
    DataNodeConfigSource source3 = helixFactory.getDataNodeConfigSource(clusterMapConfig, "different-zk", metrics);
    assertNotSame("Different ZK address should return different source", source1, source3);
  }

  /**
   * Test buildZKHelixManager with null ClusterMapConfig.
   */
  @Test
  public void testBuildZKHelixManagerWithNullConfig() {
    HelixFactory helixFactory = new HelixFactory();
    HelixManager manager = helixFactory.buildZKHelixManager(CLUSTER_NAME, INSTANCE_NAME, InstanceType.PARTICIPANT, ZK_ADDR, null);
    
    assertNotNull("Manager should not be null even with null config", manager);
  }

  /**
   * Test buildZKHelixManager with auto-registration disabled.
   */
  @Test
  public void testBuildZKHelixManagerAutoRegistrationDisabled() {
    Properties props = new Properties();
    props.setProperty("clustermap.cluster.name", CLUSTER_NAME);
    props.setProperty("clustermap.datacenter.name", "DC1");
    props.setProperty("clustermap.host.name", "localhost");
    props.setProperty("clustermap.port", "1234");
    props.setProperty("clustermap.auto.registration.enabled", "false");
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));

    HelixFactory helixFactory = new HelixFactory();
    HelixManager manager = helixFactory.buildZKHelixManager(CLUSTER_NAME, INSTANCE_NAME, InstanceType.PARTICIPANT, ZK_ADDR, clusterMapConfig);
    
    assertNotNull("Manager should not be null", manager);
  }

  /**
   * Test buildZKHelixManager with auto-registration enabled.
   */
  @Test
  public void testBuildZKHelixManagerAutoRegistrationEnabled() {
    Properties props = new Properties();
    props.setProperty("clustermap.cluster.name", CLUSTER_NAME);
    props.setProperty("clustermap.datacenter.name", "DC1");
    props.setProperty("clustermap.host.name", "localhost");
    props.setProperty("clustermap.port", "1234");
    props.setProperty("clustermap.auto.registration.enabled", "true");
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));

    HelixFactory helixFactory = new HelixFactory();
    HelixManager manager = helixFactory.buildZKHelixManager(CLUSTER_NAME, INSTANCE_NAME, InstanceType.PARTICIPANT, ZK_ADDR, clusterMapConfig);
    
    assertNotNull("Manager should not be null", manager);
  }

  /**
   * Test that HelixFactory properly handles different instance types with auto-registration.
   */
  @Test
  public void testBuildZKHelixManagerInstanceTypes() {
    Properties props = new Properties();
    props.setProperty("clustermap.cluster.name", CLUSTER_NAME);
    props.setProperty("clustermap.datacenter.name", "DC1");
    props.setProperty("clustermap.host.name", "localhost");
    props.setProperty("clustermap.port", "1234");
    props.setProperty("clustermap.auto.registration.enabled", "true");
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));

    HelixFactory helixFactory = new HelixFactory();
    
    HelixManager participantManager = helixFactory.buildZKHelixManager(CLUSTER_NAME, INSTANCE_NAME, InstanceType.PARTICIPANT, ZK_ADDR, clusterMapConfig);
    HelixManager spectatorManager = helixFactory.buildZKHelixManager(CLUSTER_NAME, INSTANCE_NAME, InstanceType.SPECTATOR, ZK_ADDR, clusterMapConfig);
    HelixManager adminManager = helixFactory.buildZKHelixManager(CLUSTER_NAME, INSTANCE_NAME, InstanceType.ADMINISTRATOR, ZK_ADDR, clusterMapConfig);
    
    assertNotNull("Participant manager should not be null", participantManager);
    assertNotNull("Spectator manager should not be null", spectatorManager);
    assertNotNull("Admin manager should not be null", adminManager);
  }

  /**
   * Test ManagerKey constructor.
   */
  @Test
  public void testManagerKeyConstructor() {
    HelixFactory.ManagerKey key = new HelixFactory.ManagerKey(CLUSTER_NAME, INSTANCE_NAME, InstanceType.PARTICIPANT, ZK_ADDR);
    
    assertNotNull("ManagerKey should not be null", key);
    // We can't directly test the private fields, but we can test equals/hashCode behavior
    HelixFactory.ManagerKey sameKey = new HelixFactory.ManagerKey(CLUSTER_NAME, INSTANCE_NAME, InstanceType.PARTICIPANT, ZK_ADDR);
    assertEquals("Keys with same parameters should be equal", key, sameKey);
  }

  /**
   * Test edge cases for ManagerKey equals method.
   */
  @Test
  public void testManagerKeyEqualsEdgeCases() {
    HelixFactory.ManagerKey key = new HelixFactory.ManagerKey(CLUSTER_NAME, INSTANCE_NAME, InstanceType.PARTICIPANT, ZK_ADDR);
    
    // Test with null values
    HelixFactory.ManagerKey keyWithNulls = new HelixFactory.ManagerKey(null, null, null, null);
    assertNotEquals("Key with nulls should not equal key with values", key, keyWithNulls);
    
    HelixFactory.ManagerKey anotherKeyWithNulls = new HelixFactory.ManagerKey(null, null, null, null);
    assertEquals("Keys with same null values should be equal", keyWithNulls, anotherKeyWithNulls);
  }
}
