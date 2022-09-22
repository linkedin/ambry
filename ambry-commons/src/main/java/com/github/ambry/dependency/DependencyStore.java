/**
 * Copyright 2022 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.dependency;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

/**
 * Dependency store contains a map from object to dependency info.
 * The object can be a string, an interface type, a class type, or anything that is unique.
 */
public class DependencyStore {

  /**
   * Store information about the dependency provider.
   * This information is internal/protected with no public access.
   */
  protected static class DependencyInfo {
    /**
     * The instance/singleton.
     */
    public Object instance;

    /**
     * The Supplier callback.
     */
    public Object supplier;

    /**
     * The factory callback.
     */
    public Object factory;

    /**
     * A set of subscribers.
     * Subscribers will be notified when the instance value has changed.
     */
    public Set<Object> subscribers;
  }

  /**
   * Map from a unique key to DependencyInfo.
   */
  private final Map<Object, DependencyInfo> keyToDependencyInfoMap = new ConcurrentHashMap<>();

  /**
   * When client calls provideSupplier(key, Supplier), when should Dependency call Supplier.get()?
   * The answer depends on whether the store is in SETUP state or ACTIVATED state.  See get() for detail.
   */
  private boolean activeState = false;

  /**
   * Get whether this store is in ACTIVATED state.
   * In SETUP state, provideSupplier(type, Supplier) will store the Supplier without calling Supplier.get().
   * In ACTIVATED state, provideSupplier(type, Supplier) converted to provideInstance(type,supplier.get()).
   * @return TRUE if in ACTIVATED state; otherwise return FALSE.
   */
  public boolean isActivated() {
    return activeState;
  }

  /**
   * Transit from SETUP state to ACTIVE state.  It does nothing if store is already in ACTIVATED state.
   * There is no transition back to the SETUP state.
   * It iterates through all existing subscribers and call them with the instances they are listening to.
   * This method is synchronized so that it executes only once.
   */
  public void activate() {
    if (activeState) return;

    synchronized (keyToDependencyInfoMap) {
      if (activeState) return;
      activeState = true;

      notifyAllSubscribers();
    }
  }

  /**
   * To publish or provide a singleton that implements a specific key.
   * This method is useful when registering a singleton that is already created.
   * This means the singleton does not have dependency on components.
   *
   * @param key The key to register.  This cannot be null.
   * @param instance The instance.  It cannot be null.
   */
  public void provideInstance(Object key, Object instance) {
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(instance, "instance");

    // The key must be registered either as a singleton, callback, or factory, but not multiple places.
    DependencyInfo info = keyToDependencyInfoMap.computeIfAbsent(key, item -> new DependencyInfo());
    updateInstance(key, instance, info);
  }

  /**
   * To publish or provide an instance supplier that create an instance of the specified key.
   * This method is useful when the supplier.get() depends on other components.  It effectively delays the instance
   * creation until all components are registered and when get() is called.
   * The supplier.get() will be called once the first time when get(key) is called and then caches the value.
   * <p>
   * In SETUP state, the supplier is stored and will not be called until get(key) is called.
   * In ACTIVATED state, the provideSupplier(supplier) is converted to provideInstance(supplier.get()).
   *
   * @param key The key to register.  This cannot be null.
   * @param supplier The supplier that returns an instance.  The supplier cannot be null.
   */
  public void provideSupplier(Object key, Supplier<? extends Object> supplier) {
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(supplier, "supplier");

    DependencyInfo info = keyToDependencyInfoMap.computeIfAbsent(key, item -> new DependencyInfo());
    if (isActivated()) {
      // In ACTIVATED state, get supplier instance and convert the call to register a singleton instance.
      Object newInstance = createSupplierInstance(key, supplier);
      updateInstance(key, newInstance, info);
    }
    else {
      // In SETUP state, store the supplier/callback.  Do not retrieve the instance until get() is called.
      info.supplier = supplier;
      info.instance = null;
      info.factory = null;
    }
  }

  /**
   * To publish or provide an instance factory that generates a new instance per get() or tryGet() call.
   *
   * @param key The key to register.  This cannot be null.
   * @param factory The factory that returns an instance.  The factory cannot be null.
   */
  public void provideFactory(Object key, Supplier<? extends Object> factory) {
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(factory, "factory");

    DependencyInfo info = keyToDependencyInfoMap.computeIfAbsent(key, item -> new DependencyInfo());
    info.supplier = null;
    info.instance = null;
    info.factory = factory;
  }

  /**
   * Helper method to update the dependency info instance and invoke subscribers.
   * @param key The key to register.  This cannot be null.
   * @param instance The instance to store.  It cannot be null.
   * @param info The parameter info.
   */
  private void updateInstance(Object key, Object instance, DependencyInfo info) {
    Object oldInstance = info.instance;
    info.instance = instance;
    info.factory = null;
    info.supplier = null;

    if (isActivated() && oldInstance != instance) {
      notifySubscribers(key, oldInstance, instance, info);
    }
  }

  /**
   * Remove an instance provider added using provideX().
   * @param key The key to remove.
   * @return TRUE if the key is found and removed; FALSE if not found.
   */
  public boolean remove(Object key) {
    Objects.requireNonNull(key, "key");

    // Remove from both singleton and callback maps.
    DependencyInfo info = keyToDependencyInfoMap.remove(key);
    if (isActivated() && info != null && info.instance != null) {
      // Invoke the listeners.
      notifySubscribers(key, info.instance, null, info);
    }

    return info != null;
  }

  /**
   * Check whether the given key has been registered so that get() will get an instance if succeeded.
   * @param key The key to check.
   * @return TRUE if key has provider so get() will return an instance; FALSE if no provider.
   */
  public boolean hasProvider(Object key) {
    Objects.requireNonNull(key, "key");

    // If dependency is explicitly registered, it must be an instance, supplier, or factory.
    DependencyInfo info = keyToDependencyInfoMap.get(key);
    boolean hasProvider = (info != null) && (info.instance != null || info.supplier != null || info.factory != null);

    // If not explicitly registered, check whether it's a class with DependencyProvider annotation.
    if (!hasProvider) {
      hasProvider = (getDependencyProviderAnnotation(key) != null);
    }

    return hasProvider;
  }

  /**
   * Get the instance for the specific key.  Throw DependencyNotFoundException when key is not found.
   * Use tryGet() if you want null instead of DependencyNotFoundException when key is not found.
   *
   * @param key The key to get.
   * @return The instance registered using the key.
   */
  public Object get(Object key) {
    Objects.requireNonNull(key, "key");
    Object instance = tryGet(key);
    if (instance == null) {
      // There is no registered provider for the key.
      throw new DependencyNotFoundException(key, "Interface type " + key
          + " has no provider and the interface no has DependencyProvider annotation.");
    }

    return instance;
  }

  /**
   * Get the instance for the specific key.  Return null if object is not found.
   *
   * @param key The key to get.
   * @return The instance for the given key.  NULL if not found (no provider).
   */
  public Object tryGet(Object key) {
    Objects.requireNonNull(key, "key");

    // If interface is registered using provideX(), then return the instance.
    DependencyInfo info = keyToDependencyInfoMap.get(key);
    if (info != null) {
      if (info.instance != null) {
        return info.instance;
      }
      if (info.supplier != null) {
        Object newInstance = createSupplierInstance(key, (Supplier<?>) info.supplier);
        updateInstance(key, newInstance, info);
        return newInstance;
      }
      if (info.factory != null) {
        return createSupplierInstance(key, (Supplier<?>) info.factory);
      }
    }

    // No explicit registration using provideX().  Look for the DependencyProvider annotation
    DependencyProvider annotation = getDependencyProviderAnnotation(key);
    if (annotation != null) {
      Object newInstance = createAnnotatedInstance((Class<?>) key, annotation);
      provideInstance(key, newInstance);
      return newInstance;
    }

    return null;
  }

  /**
   * Create an instance using a supplier/callback.  It throws DependencyException if supplier.get() throws.
   *
   * @param key The key.  Caller already verified that it is not null.  It's for logging purpose.
   * @param supplier The callback that creates the instance.
   * @return The newly created instance.
   */
  private Object createSupplierInstance(Object key, Supplier<?> supplier) {
    try {
      return supplier.get();
    }
    catch (Exception ex) {
      throw new DependencyException(key, "Unable to create new instance of " + key +
          " using the Supplier callback.", ex);
    }
  }

  /**
   * If key is a class type, and it contains the DependencyProvider annotation, return the annotation.
   * @param key The key.
   * @return DependencyProvider annotation if found; null if not dependency annotation found or key is not a class.
   */
  private DependencyProvider getDependencyProviderAnnotation(Object key) {
    DependencyProvider annotation = null;
    if (key instanceof Class<?>) {
      Class<?> interfaceClass = (Class<?>) key;
      annotation = interfaceClass.getAnnotation(DependencyProvider.class);
    }
    return annotation;
  }

  /**
   * Create an instance for the given key class and the annotation.
   *
   * @param keyClass The class of the key.
   * @param annotation The DependencyProvider annotation attached to the class.  It has been null checked.
   * @return An instance that implements TInterface.
   */
  private Object createAnnotatedInstance(Class<?> keyClass, DependencyProvider annotation) {
    // If keyClass is an interface or abstract class, it requires "instanceClass" parameter.
    // If keyClass is a class, "instanceClass" is optional.
    Class<?> instanceClass = annotation.instanceClass();
    if (instanceClass == null || instanceClass == void.class) {
      // No instanceClass specified.  Make sure keyClass is a class, not an interface or abstract class.
      if (keyClass.isInterface() || Modifier.isAbstract(keyClass.getModifiers())) {
        throw new DependencyException(keyClass, "Dependency annotation specified in interface or abstract class "
            + keyClass + ", but DependencyProvider annotation has missing instanceClass value.");
      }

      // Assume keyClass is a class type.
      instanceClass = keyClass;
    }

    Object newInstance;
    try {
      newInstance = createClassInstance(instanceClass);
    }
    catch (Exception ex) {
      throw new DependencyException(keyClass, "Unable to create new instance of class " + instanceClass.getName(), ex);
    }

    // Verify the newInstance implements or extends the key class.
    if (!keyClass.isInstance(newInstance)) {
      throw new DependencyException(keyClass, "The class " + instanceClass.getName() + " does not implement "
          + keyClass.getName());
    }

    return newInstance;
  }

  /**
   * Create an instance of the given class using reflection.
   * It invokes the constructor marked with DependencyConstructor annotation or parameterless constructor.
   *
   * @param instanceClass The class to create a new instance.  Caller verified that it is not null.
   * @return The newly created instance.
   */
  private Object createClassInstance(Class<?> instanceClass)
      throws InstantiationException, IllegalAccessException, InvocationTargetException {
    // Use reflection to find the constructor marked with @DependencyConstructor
    Constructor<?> useConstructor = null;
    for (Constructor<?> constructor : instanceClass.getConstructors()) {
      if (constructor.getAnnotation(DependencyConstructor.class) != null) {
        useConstructor = constructor;
        break;
      }
    }

    // If no DependencyConstructor found, or constructor is parameterless, just call the parameterless constructor.
    if (useConstructor == null || useConstructor.getParameterCount() == 0) {
      return instanceClass.newInstance();
    }

    // There is at least one parameter in the constructor.  Need to resolve them.
    Object[] parameterValues = new Object[useConstructor.getParameterCount()];
    int i = 0;
    for (Parameter parameter : useConstructor.getParameters()) {
      // If parameter has name annotation, lookup by the name; otherwise, lookup value by its type.
      DependencyName nameAnnotation = parameter.getAnnotation(DependencyName.class);
      parameterValues[i] = (nameAnnotation == null) ? get(parameter.getParameterizedType()) : get(nameAnnotation.value());
      i++;
    }

    return useConstructor.newInstance(parameterValues);
  }

  /**
   * Subscribe or listen to all changes to the specific key registration.
   *
   * @param key The instance key to listen to.
   * @param callback The callback or listener to add.
   */
  public void subscribe(Object key, BiConsumer<Object, Object> callback) {
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(callback, "callback");

    DependencyInfo info = keyToDependencyInfoMap.computeIfAbsent(key, item -> new DependencyInfo());
    if (info.subscribers == null) {
      info.subscribers = ConcurrentHashMap.newKeySet();
    }
    info.subscribers.add(callback);
  }

  /**
   * Unsubscribe or remove the listener for the specific key.
   * @param key The instance key to unsubscribe.
   * @param callback The callback or listener to remove.
   */
  public void unsubscribe(Object key, BiConsumer<Object, Object> callback) {
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(callback, "callback");

    DependencyInfo info = keyToDependencyInfoMap.get(key);
    if (info != null && info.subscribers != null) {
      info.subscribers.remove(callback);
    }
  }

  /**
   * Call the subscribers or listeners when the dependency value has changed.
   *  The subscriber callbacks are synchronized so no 2 threads will invoke them at the same time.
   *
   * @param key The key for logging purpose.
   * @param oldInstance The old instance.  It may be null.
   * @param newInstance The new instance.  It may be null.
   */
  protected void notifySubscribers(Object key, Object oldInstance, Object newInstance, DependencyInfo info) {
    if (info.subscribers != null) {
      // Subscriber callbacks are synchronized at DependencyInfo level.
      synchronized (info) {
        for (Object callback : info.subscribers) {
          try {
            BiConsumer<Object, Object> subscriber = (BiConsumer<Object, Object>) callback;
            subscriber.accept(oldInstance, newInstance);
          } catch (Exception ex) {
            // If any subscriber has issue, abort.
            throw new DependencyException(key, "The interface " + key
                + " instance has changed but the subscriber throws an exception.", ex);
          }
        }
      }
    }
  }

  /**
   * When transitioning from SETUP state to ACTIVATED state, all subscribers will be notified.
   * It iterates through all existing subscribers and call them with the instance they are listening to.
   */
  public void notifyAllSubscribers() {
    for (Map.Entry<Object, DependencyInfo> pair : keyToDependencyInfoMap.entrySet()) {
      DependencyInfo info = pair.getValue();

      // Notify all subscribers.  If it's a factory provider, do not call the subscribers.
      if (info.factory == null && info.subscribers != null && info.subscribers.size() > 0) {
        Object instance = tryGet(pair.getKey());
        if (instance != null) {
          notifySubscribers(pair.getKey(), null, instance, info);
        }
      }
    }
  }
}
