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
 * DependencyName annotation is used with DependencyConstructor annotation to specify the dependency name of a
 * parameter.  Without the name, the parameter type will be used to get the value.
 * For example:
 * <pre>{@code
 *    // Same as Dependency.provideInstance(ClassA.class, new ClassA(Dependency.get("DependentName")));
 *    @DependencyProvider
 *    class ClassA {
 *      @DependencyConstrutor
 *      ClassA(@DependencyName("DependentName") AnotherDependent dependent) { }
 *    }
 *
 *    // Same as new ClassA(Dependency.get(MyDependent.class), Dependency.get("ABC"), Dependency.get(Class2.class))
 *    @DependencyProvider
 *    class ClassA {
 *      @DependencyConstrutor
 *      ClassA(MyDependent dependent, @DependencyName("ABC") ABCClass myABCInstance, Class2 dependent2) { }
 *    }
 * }</pre>
 */
@Documented
@Target(ElementType.PARAMETER)
@Retention(RetentionPolicy.RUNTIME)
public @interface DependencyName {
  /**
   * The unique name of the Dependency.
   */
  String value();
}
