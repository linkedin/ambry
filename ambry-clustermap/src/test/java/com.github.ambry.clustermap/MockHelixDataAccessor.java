/*
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.I0Itec.zkclient.DataUpdater;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.MaintenanceSignal;
import org.apache.helix.model.Message;
import org.apache.helix.model.PauseSignal;
import org.apache.helix.model.StateModelDefinition;


public class MockHelixDataAccessor implements HelixDataAccessor {
  private final String clusterName;
  private final PropertyKey.Builder propertyKeyBuilder;

  MockHelixDataAccessor(String clusterName) {
    this.clusterName = clusterName;
    propertyKeyBuilder = new PropertyKey.Builder(clusterName);
  }

  @Override
  public boolean createStateModelDef(StateModelDefinition stateModelDef) {
    throw new UnsupportedOperationException("Unsupported in MockHelixDataAccessor");
  }

  @Override
  public boolean createControllerMessage(Message message) {
    throw new UnsupportedOperationException("Unsupported in MockHelixDataAccessor");
  }

  @Override
  public boolean createControllerLeader(LiveInstance leader) {
    throw new UnsupportedOperationException("Unsupported in MockHelixDataAccessor");
  }

  @Override
  public boolean createPause(PauseSignal pauseSignal) {
    throw new UnsupportedOperationException("Unsupported in MockHelixDataAccessor");
  }

  @Override
  public boolean createMaintenance(MaintenanceSignal maintenanceSignal) {
    throw new UnsupportedOperationException("Unsupported in MockHelixDataAccessor");
  }

  @Override
  public <T extends HelixProperty> boolean setProperty(PropertyKey key, T value) {
    throw new UnsupportedOperationException("Unsupported in MockHelixDataAccessor");
  }

  @Override
  public <T extends HelixProperty> boolean updateProperty(PropertyKey key, T value) {
    throw new UnsupportedOperationException("Unsupported in MockHelixDataAccessor");
  }

  @Override
  public <T extends HelixProperty> boolean updateProperty(PropertyKey key, DataUpdater<ZNRecord> updater, T value) {
    throw new UnsupportedOperationException("Unsupported in MockHelixDataAccessor");
  }

  @Override
  public <T extends HelixProperty> T getProperty(PropertyKey key) {
    //return (T) (new HelixProperty("id"));
    throw new UnsupportedOperationException("Unsupported in MockHelixDataAccessor");
  }

  @Override
  public <T extends HelixProperty> List<T> getProperty(List<PropertyKey> keys) {
    throw new UnsupportedOperationException("Unsupported in MockHelixDataAccessor");
  }

  @Override
  public <T extends HelixProperty> List<T> getProperty(List<PropertyKey> keys, boolean throwException) {
    return Collections.emptyList();
  }

  @Override
  public boolean removeProperty(PropertyKey key) {
    throw new UnsupportedOperationException("Unsupported in MockHelixDataAccessor");
  }

  @Override
  public HelixProperty.Stat getPropertyStat(PropertyKey key) {
    throw new UnsupportedOperationException("Unsupported in MockHelixDataAccessor");
  }

  @Override
  public List<HelixProperty.Stat> getPropertyStats(List<PropertyKey> keys) {
    return Collections.emptyList();
  }

  @Override
  public List<String> getChildNames(PropertyKey key) {
    return Collections.emptyList();
  }

  @Override
  public <T extends HelixProperty> List<T> getChildValues(PropertyKey key) {
    throw new UnsupportedOperationException("Unsupported in MockHelixDataAccessor");
  }

  @Override
  public <T extends HelixProperty> List<T> getChildValues(PropertyKey key, boolean throwException) {
    throw new UnsupportedOperationException("Unsupported in MockHelixDataAccessor");
  }

  @Override
  public <T extends HelixProperty> Map<String, T> getChildValuesMap(PropertyKey key) {
    throw new UnsupportedOperationException("Unsupported in MockHelixDataAccessor");
  }

  @Override
  public <T extends HelixProperty> Map<String, T> getChildValuesMap(PropertyKey key, boolean throwException) {
    return Collections.emptyMap();
  }

  @Override
  public <T extends HelixProperty> boolean[] createChildren(List<PropertyKey> keys, List<T> children) {
    throw new UnsupportedOperationException("Unsupported in MockHelixDataAccessor");
  }

  @Override
  public <T extends HelixProperty> boolean[] setChildren(List<PropertyKey> keys, List<T> children) {
    throw new UnsupportedOperationException("Unsupported in MockHelixDataAccessor");
  }

  @Override
  public <T extends HelixProperty> boolean[] updateChildren(List<String> paths, List<DataUpdater<ZNRecord>> updaters,
      int options) {
    throw new UnsupportedOperationException("Unsupported in MockHelixDataAccessor");
  }

  @Override
  public PropertyKey.Builder keyBuilder() {
    return propertyKeyBuilder;
  }

  @Override
  public BaseDataAccessor<ZNRecord> getBaseDataAccessor() {
    throw new UnsupportedOperationException("Unsupported in MockHelixDataAccessor");
  }
}
