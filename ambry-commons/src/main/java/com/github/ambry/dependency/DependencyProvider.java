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

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * DependencyProvider annotation is used on a class or interface to implicitly register/provide a dependency.
 * When Dependency.get() is called, get() checks for explicitly registered instances using provideX() first.
 * If not found, get() uses reflection on the class parameter to look for the DependencyProvider annotation.
 * If annotation found, get() will use reflection to create a new instance, then caches the new instance in store.
 *<p>
 * For now, the class constructor must be parameterless or annotated using the DependencyConstructor annotation.
 * When DependencyProvider annotation is used on an interface or an abstract class (both cannot be instantiated),
 * the "instanceClass" parameter is required to specify the class to instantiate.
 * An annotation example of keyed by interface type.
 * <pre>{@code
 *    // Same as Dependency.provideInstance(InterfaceA.class, new ClassA());
 *    @DependencyProvider (instanceClass = ClassA.class)
 *    interface InterfaceA { }
 *
 *    class ClassA implements InterfaceA { }
 *
 *    public void consumerMethod() {
 *      InterfaceA instance = Dependency.get(InterfaceA.class);  // will return "new ClassA()"
 *    }
 * }</pre>
 *
 * More examples:
 * <pre>{@code
 *   // Same as Dependency.provideInstance(IMyService.class, new MyServiceImpl());
 *   @DependencyProvider(instanceClass = MyServiceImpl)
 *   interface IMyService { ... }
 *
 *   // Same as Dependency.provideInstance(AbstractService.class, new MyService());
 *   @DependencyProvider(instanceClass = MyService)
 *   class abstract AbstractService { ... }
 *
 *   // Same as Dependency.provideInstance(MyService.class, new MyService());
 *   @DependencyProvider
 *   class MyService { ... }
 * }</pre>
 */
@Documented
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface DependencyProvider {
  /**
   * Optional.  Applied only when annotating an interface.
   * When annotating an interface, this property specifies the class that implements this interface.
   * The class will be instantiated by Dependency.get() using its parameterless constructor.
   */
  Class<?> instanceClass() default void.class;
}
