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
 * DependencyConstructor annotation is used with the DependencyProvider annotation to indicate which constructor to use.
 * When a class has multiple constructors, how does DependencyProvider know which constructor to use?
 * <p>
 * With @DependencyConstructor specified, DependencyProvider will use the annotated constructor.
 * Without @DependencyConstructor, DependencyProvider will use the parameterless constructor whether it exists or not.
 * <pre>{@code
 *    // Same as Dependency.provideInstance(ClassA.class, new ClassA(Dependency.get(MyDependent.class)));
 *    @DependencyProvider
 *    class ClassA {
 *      ClassA() { }
 *
 *      @DependencyConstrutor
 *      ClassA(MyDependent dependent) { }
 *
 *      ClassA(OtherParameter parameter) { }
 *    }
 *
 *    // Same as Dependency.provideInstance(ClassA.class, new ClassA(Dependency.get("DependentName")));
 *    @DependencyProvider
 *    class ClassA {
 *      @DependencyConstrutor
 *      ClassA(@DependencyName("DependentName") AnotherDependent dependent) {
 *      }
 *    }
 * }</pre>
 */
@Documented
@Target(ElementType.CONSTRUCTOR)
@Retention(RetentionPolicy.RUNTIME)
public @interface DependencyConstructor {
}
