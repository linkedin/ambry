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

import java.util.function.BiConsumer;
import java.util.function.Supplier;

/**
 * A simple dependency injection alternative to the popular the dependency injection like Google Guice.
 * <p>
 * It acts like central key-value store of instances and make the instances available globally.
 * The key can be a class or a string, like Guice.  The value can be a singleton instance, a supplier, or a factory.
 * If the key is class, that class can be annotated to be a run-time dependency provider.  See examples below.
 * <p>
 * The example below illustrate the problem of not having dependency injection.
 * ClassA needs to use InterfaceX.  Without dependency, one common approach is pass an instance of
 * InterfaceX to ClassA constructor. If ClassA has many dependencies, all dependencies are required to passed to
 * ClassA constructor.  That can get messy.
 * In some cases, a constructor has to accept parameters required to construct dependent objects.
 * For example, the ParentClass class needs to instantiate the MyDependent class in its methods.  MyDependent
 * constructor needs a ClassB instance that is passed in, so ParentClass constructor needs to accept a ClassB instance.
 * From the client perspective, it doesn't make sense to pass ClassB instance to ParentClass because it's not related.
 * Without dependency, passing objects becomes complex, difficult to use, and inefficient because it forces
 * pre-creation of dependent objects.
 * <p>
 * Code example without dependency injection:
 * <pre>{@code
 *    class MyDependent {
 *      public MyDependent(ClassB instanceB) { ... }
 *    }
 *
 *    class ParentClass {
 *        ClassA instanceA;
 *
 *        public ParentClass(ClassA instanceA, ClassB instanceB) {  // passing ClassB to ParentClass makes no sense.
 *           this.instanceA = instanceA;
 *           if (someCondition) this.myDependent = new MyDependent(instanceB);
 *        }
 *    }
 *
 *    public ParentClass getParent() {
 *      return new ParentClass(new ClassA(), new ClassB());
 *    }
 * }</pre>
 *
 * Example with Dependency.  Assume both ClassA and ClassB are singleton registered as Dependency providers.
 * Notice the ParentClass constructor is clean since the dependent objects are globally available.
 * <pre>{@code
 *    class MyDependent {
 *      public MyDependent(ClassB instanceB) { ... }
 *    }
 *
 *    class ParentClass {
 *        public ParentClass() {
 *           this.instanceA = Dependency.get(ClassA.class);
 *           if (someCondition) this.myDependent = new MyDependent(Dependency.get(ClassB.class));
 *        }
 *    }
 *
 *    public void oneTimeSetup() {
 *      Dependency.provideInstance(ClassA.class, new ClassA());
 *      Dependency.provideInstance(ClassB.class, new ClassB());
 *      Dependency.provideInstance(ParentClass.class, () -> new ParentClass());
 *    }
 *
 *    public ParentClass getParent() {
 *      return Dependency.get(ParentClass.class);
 *    }
 * }</pre>
 *
 * To use Dependency, the instances must be registered.  After registration, the instances are available through get().
 * There are 4 ways to register instances:
 * - call Dependency.provideInstance(instance)
 * - call Dependency.provideSupplier(SupplierCallback);   // The callback will be invoked when get() is called.
 * - call Dependency.provideFactory(SupplierFactory);  // This registers a factory, not a singleton.
 * - Using DependencyProvider annotation.  This approach is also known as implicit registration.
 *
 * Call get() to retrieve the instances.  get() will check the singleton registration, the supplier registration,
 * and factory registration.  If none of them found, it checks whether the class has DependencyProvider annotation.
 *
 * Explicit registration using the {@code provideX()} method.
 * This is usually done when application starts.  The registration key can be a class or by a unique string name.
 * For example:
 * <pre>{@code
 *    public void startup() {
 *      // To register a pre-created singleton by class.
 *      Dependency.provideInstance(InterfaceA.class, new ClassA());  // class ClassA implements InterfaceA
 *
 *      // To register a pre-created singleton by name.
 *      Dependency.provideInstance("InterfaceA", new ClassA());
 *
 *      // To register a supplier/callback instead of an instance.
 *      // get() will invoke the supplier/callback only once and caches the instance as singleton, so subsequent
 *      // get(Interface.class) calls will return the same instance.
 *      Dependency.provideSupplier(InterfaceA.class, () -> new ClassA());
 *
 *      // To register a supplier using a name.
 *      Dependency.provideSupplier("InterfaceA", () -> new ClassA());
 *
 *      // To register a factory by type.  Unlike supplier, get() will invoke the factory callback each time.
 *      Dependency.provideFactory(ClassZ.class, () -> new ClassZ());
 *
 *      // To register a factory by name.
 *      Dependency.provideFactory("InterfaceZ", () -> new ClassZ());
 *    }
 *
 *    void consumerMethod() {
 *      // Get instance by class.
 *      InterfaceA instance = Dependency.get(InterfaceA.class);
 *
 *      // Get instance by name.
 *      InterfaceA instance = Dependency.get("InterfaceA");
 *    }
 * }</pre>
 *
 * 2. Implicit registration using the DependencyProvider annotation.  It is same as provideSupplier().
 *    When get() is called, get() checks for explicit registration first.  If found, return the instance.
 *    If key not explicitly registered using provideX() and the key is a class, get() uses reflection on the  key
 *    to look for the DependencyProvider annotation.  If annotation found, get() instantiates the class and
 *    caches the instance.  Examples of various DependencyProvider annotations.
 * <pre>{@code
 *    // Same as Dependency.provideSupplier(Sample.class, () -> new Sample())
 *    @DependencyProvider
 *    public class Sample {
 *    }
 *
 *    // Same as Dependency.provideSupplier(ISample.class, () -> new Sample())
 *    @DependencyProvider(instanceClass=Sample.class)
 *    public interface ISample { }
 *
 *    // Same as Dependency.provideInstance(ClassA.class, new ClassA(Dependency.get(MyDependent.class)));
 *    @DependencyProvider
 *    class ClassA {
 *      @DependencyConstructor
 *      public ClassA(MyDependent dependent) { }
 *    }
 *
 *    // Same as Dependency.provideInstance(ClassB.class, new ClassB(Dependency.get("MyName")));
 *    @DependencyProvider
 *    class ClassB {
 *      @DependencyConstrutor
 *      public ClassB(@DependencyName("MyName") AnotherDependent dependent) { }
 *    }
 *
 *    // Same as Dependency.provideInstance(ClassC.class,
 *    //          new ClassC(Dependency.get(MyDependent.class), Dependency.get("ABC"), Dependency.get(Class2.class))
 *    @DependencyProvider
 *    class ClassC {
 *      public ClassC(OtherParameter parameter) { }

 *      @DependencyConstructor
 *      public ClassC(MyDependent dependent, @DependencyName("ABC") ABCClass myABCInstance, Class2 dependent2) { }
 *    }
 * }</pre>
 */
public final class Dependency {

  /**
   * The shared instance of DependencyStore to use.
   */
  static DependencyStore dependencyStore = new DependencyStore();

  /**
   * To register a pre-created singleton by its class.
   *
   * @param instanceType The type (interface, base class, or class itself) to register.  This cannot be null.
   * @param singletonInstance The singleton that implements instanceType.  It cannot be null.
   * @param <TInstance> The instance generic type.
   */
  public static <TInstance> void provideInstance(Class<TInstance> instanceType, TInstance singletonInstance) {
    dependencyStore.provideInstance(instanceType, singletonInstance);
  }

  /**
   * To register a pre-created singleton by a unique name.
   *
   * @param instanceName The unique name to register.  This cannot be null.
   * @param singletonInstance The singleton assigned to this unique name.  It cannot be null.
   */
  public static void provideInstance(String instanceName, Object singletonInstance) {
    dependencyStore.provideInstance(instanceName, singletonInstance);
  }

  /**
   * To register an instance supplier that generates an instance when get() is called, keyed by the instance type.
   * This method is useful when the supplier.get() depends on other components and calls Dependency.get().
   * It effectively delays the instance creation until its dependencies are registered before get() is called.
   * <p>
   * Normally the supplier callback is stored in DependencyStore until get() is called.  Calling get() means all
   * dependencies should have been registered.  That's why calling get() changes the DependencyStore state to Activated.
   * In the activated state, calling providerSupplier(Supplier) redirects to providerInstance(Supplier.get())
   * instead of storing the callback in DependencyStore.
   *
   * @param instanceType The instance type to register.  This cannot be null.
   * @param supplier The supplier to invoke when get() is called.  The callback is called once and the result is
   *                 cached.  The callback cannot be null.
   * @param <TInstance> The instance generic type.
   */
  public static <TInstance> void provideSupplier(Class<TInstance> instanceType,
      Supplier<? extends TInstance> supplier) {
    dependencyStore.provideSupplier(instanceType, supplier);
  }

  /**
   * To register an instance supplier that generate an instance when get() is called, keyed by a unique name.
   * This method is useful when the supplier.get() depends on other components, so it effectively delays the instance
   * creation until its dependencies are registered before get() is called.
   *
   * @param instanceName The unique name to register.  This cannot be null.
   * @param supplier The supplier to invoke when get() is called.  The callback is called once and the result is
   *                 cached.  The callback cannot be null.
   */
  public static void provideSupplier(String instanceName, Supplier<? extends Object> supplier) {
    dependencyStore.provideSupplier(instanceName, supplier);
  }

  /**
   * To register an instance factory that generate a new instance per get() call, keyed by instance class.
   * The factory.get() will be called everytime the get(class) is called.
   *
   * @param instanceType The class to register.  This cannot be null.
   * @param factory The factory to invoke each time get() is called.
   * @param <TInstance> The instance generic type.
   */
  public static <TInstance> void provideFactory(Class<TInstance> instanceType,
      Supplier<? extends TInstance> factory) {
    dependencyStore.provideFactory(instanceType, factory);
  }

  /**
   * To register an instance factory that generate a new instance per get() call, keyed by a unique name.
   * The factory.get() will be called everytime the get(name) is called.
   *
   * @param instanceName The unique name to register.  This cannot be null.
   * @param factory The factory to invoke each time get() is called.
   */
  public static void provideFactory(String instanceName, Supplier<? extends Object> factory) {
    dependencyStore.provideFactory(instanceName, factory);
  }

  /**
   * Check whether the given instance type has been registered so that get() will return an instance.
   * @param instanceType The instance class to check.
   * @return TRUE if class has provider so get() will return an instance; FALSE if no provider.
   */
  public static boolean hasProvider(Class<?> instanceType) {
    return dependencyStore.hasProvider(instanceType);
  }

  /**
   * Check whether the given instance name has been registered so that get() will return an instance.
   * @param instanceName The unique name to check.
   * @return TRUE if instance has provider so get() will return an instance; FALSE if no provider.
   */
  public static boolean hasProvider(String instanceName) {
    return dependencyStore.hasProvider(instanceName);
  }

  /**
   * Get the instance for the specific instance type.
   * It automatically activates the DependencyStore.  During activation, the subscribers will be notified.
   * After activation, all providerSupplier() calls will be converted to provideInstance(supplier.get()).
   * <p>
   * This method will throw DependencyException if it failed to construct the dependent object or subscriber throws.
   * This method will throw DependencyNotFoundException if the class is not found.
   * Use hasProvider() if you want to check whether class has a provider.
   *
   * @param instanceType The instance class to get.
   * @param <TInstance> The instance generic type.
   * @return The instance for the given class.  Throw DependencyNotFoundException if not found.
   */
  public static <TInstance> TInstance get(Class<? extends TInstance> instanceType) {
    dependencyStore.activate();
    return (TInstance) dependencyStore.get(instanceType);
  }

  /**
   * Get the instance by a unique name.  See get(Class<?>) for detail.
   *
   * @param instanceName A unique name of the instance to get.
   * @param <TInstance> The instance generic type.
   * @return The instance for the given name.  Throw DependencyNotFoundException if not found.
   */
  public static <TInstance> TInstance get(String instanceName) {
    dependencyStore.activate();
    return (TInstance) dependencyStore.get(instanceName);
  }

  /**
   * Try to get the instance for the specific class.
   * Unlike get(), tryGet() will return null instead of throwing DependencyNotFoundException if instance is not found.
   *
   * @param instanceType The instance class to get.
   * @return The instance for the given class.  NULL if not found (no provider).
   */
  public static <TInstance> TInstance tryGet(Class<? extends TInstance> instanceType) {
    dependencyStore.activate();
    return (TInstance) dependencyStore.tryGet(instanceType);
  }

  /**
   * Try to get the instance for the specific instance name.
   * Unlike get(), tryGet() will return null instead of throwing DependencyNotFoundException if instance is not found.
   *
   * @param instanceName The instance name to get.
   * @return The instance for the given class.  NULL if not found (no provider).
   */
  public static <TInstance> TInstance tryGet(String instanceName) {
    dependencyStore.activate();
    return (TInstance) dependencyStore.tryGet(instanceName);
  }

  /**
   * To remove a provider registered using provideX(Class<?>).
   * @param instanceType The instance class to remove.
   * @return TRUE if the instance class is found and removed; FALSE if not found.
   */
  public static boolean remove(Class<?> instanceType) {
    return dependencyStore.remove(instanceType);
  }

  /**
   * To remove a provider registered using provideX(String name).
   * @param instanceName The unique instance name to remove.
   * @return TRUE if the instance is found and removed; FALSE if not found.
   */
  public static boolean remove(String instanceName) {
    return dependencyStore.remove(instanceName);
  }

  /**
   * Subscribe or listen to all changes to the specific instance type.
   * The subscribers are called during activation if the subscriber is registered before activation.
   * After activation, subscriber will be called when the instance has changed when provideX() or remove() is called.
   * <p>
   * WARNING: The subscriber callback will be stored in Dependency which is global and static.
   * Objects referenced in the callback will not be freed by JVM Garbage Collection (GC) until unsubscribed.
   * The subscriber callback must:
   * - pay attention to the objects referenced in the callback function.
   * - make sure to call unsubscribe() as soon as it no longer needs to listen.
   *
   * @param instanceType The instance type to listen to.
   * @param subscriber The callback to invoke when the value changes.
   */
  public static void subscribe(Class<?> instanceType, BiConsumer<Object, Object> subscriber) {
    dependencyStore.subscribe(instanceType, subscriber);
  }

  /**
   * Subscribe or listen to all changes to the specific instance name.
   * The subscribers are called during activation if the subscriber is registered before activation.
   * After activation, subscriber will be called when the instance has changed when provideX() or remove() is called.
   *
   * @param instanceName The instance name to listen to.
   * @param subscriber The callback to invoke when the value changes.
   */
  public static void subscribe(String instanceName, BiConsumer<Object, Object> subscriber) {
    dependencyStore.subscribe(instanceName, subscriber);
  }

  /**
   * Unsubscribe or remove a listener from listening to a specific instance type.
   * @param instanceType The instance type to remove listener.
   * @param subscriber The subscriber to remove.
   */
  public static void unsubscribe(Class<?> instanceType, BiConsumer<Object, Object> subscriber) {
    dependencyStore.unsubscribe(instanceType, subscriber);
 }

  /**
   * Unsubscribe or remove a listener from listening to a specific instance name.
   * @param instanceName The instance name to remove listener.
   * @param subscriber The subscriber to remove.
   */
  public static void unsubscribe(String instanceName, BiConsumer<Object, Object> subscriber) {
    dependencyStore.unsubscribe(instanceName, subscriber);
  }
}
