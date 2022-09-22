package com.github.ambry.dependency;

import com.github.ambry.utils.TestUtils;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DependencyTest {

  // This class is for testing purpose.
  static class TestClass {}

  @DependencyProvider
  static class TestAnnotatedClass {}

  @Before
  public void beforeTest() {
    // Before each test, clear the store in Dependency.
    Dependency.dependencyStore = new DependencyStore();
  }

  @AfterClass
  public static void afterClass() {
    // After all tests, reset the DependencyStore.
    Dependency.dependencyStore = new DependencyStore();
  }

  @Test
  public void testProvideInstance() {
    Dependency.provideInstance(TestClass.class, new TestClass());
    Assert.assertTrue(Dependency.get(TestClass.class) instanceof TestClass);

    Dependency.provideInstance("MyTestClass", new TestClass());
    Assert.assertTrue(Dependency.get("MyTestClass") instanceof TestClass);
  }

  @Test
  public void testProvideSupplier() throws DependencyNotFoundException, DependencyException {
    Dependency.provideSupplier(TestClass.class, TestClass::new);
    Assert.assertTrue(Dependency.get(TestClass.class) instanceof TestClass);
    Assert.assertEquals(Dependency.get(TestClass.class), Dependency.get(TestClass.class));

    Dependency.provideSupplier("MyTestClass", TestClass::new);
    Assert.assertTrue(Dependency.get("MyTestClass") instanceof TestClass);
    Assert.assertEquals((Object) Dependency.get("MyTestClass"), Dependency.get("MyTestClass"));
  }

  @Test
  public void testProvideFactory() throws DependencyNotFoundException, DependencyException {
    Dependency.provideFactory(TestClass.class, TestClass::new);
    Assert.assertTrue(Dependency.get(TestClass.class) instanceof TestClass);
    Assert.assertNotEquals(Dependency.get(TestClass.class), Dependency.get(TestClass.class));

    Dependency.provideFactory("MyFactory", TestClass::new);
    Assert.assertTrue(Dependency.get("MyFactory") instanceof TestClass);
    Assert.assertNotEquals(Dependency.get("MyFactory"), Dependency.get("MyFactory"));
  }

  @Test
  public void testHasProvider() {
    Assert.assertFalse(Dependency.hasProvider(TestClass.class));
    Dependency.provideInstance(TestClass.class, new TestClass());
    Assert.assertTrue(Dependency.hasProvider(TestClass.class));

    Assert.assertFalse(Dependency.hasProvider("MyInstanceName"));
    Dependency.provideInstance("MyInstanceName", new TestClass());
    Assert.assertTrue(Dependency.hasProvider("MyInstanceName"));
  }

  @Test
  public void testGet() {
    // Use DependencyProvider annotation.
    Assert.assertTrue(Dependency.get(TestAnnotatedClass.class) instanceof TestAnnotatedClass);

    Assert.assertNotNull(TestUtils.getException(() -> Dependency.get(TestClass.class)));
    Dependency.provideInstance(TestClass.class, new TestClass());
    Assert.assertTrue(Dependency.get(TestClass.class) instanceof TestClass);

    Assert.assertNotNull(TestUtils.getException(() -> Dependency.get("InstancedName")));
    Dependency.provideInstance("InstancedName", new TestClass());
    Assert.assertTrue(Dependency.get("InstancedName") instanceof TestClass);
  }

  @Test
  public void testTryGet() {
    // Use DependencyProvider annotation.
    Assert.assertTrue(Dependency.tryGet(TestAnnotatedClass.class) instanceof TestAnnotatedClass);

    Assert.assertNull(Dependency.tryGet(TestClass.class));
    Dependency.provideInstance(TestClass.class, new TestClass());
    Assert.assertTrue(Dependency.tryGet(TestClass.class) instanceof TestClass);

    Assert.assertNull(Dependency.tryGet("InstancedName"));
    Dependency.provideInstance("InstancedName", new TestClass());
    Assert.assertTrue(Dependency.tryGet("InstancedName") instanceof TestClass);
  }

  @Test
  public void testRemove() {
    Dependency.provideInstance(TestClass.class, new TestClass());
    Assert.assertTrue(Dependency.get(TestClass.class) instanceof TestClass);
    Dependency.remove(TestClass.class);
    Assert.assertFalse(Dependency.hasProvider(TestClass.class));

    Dependency.provideInstance("ClassName", new TestClass());
    Assert.assertTrue(Dependency.get("ClassName") instanceof TestClass);
    Dependency.remove("ClassName");
    Assert.assertFalse(Dependency.hasProvider("ClassName"));
  }

  @Test
  public void testSubscribeUnsubscribeByClass() {
    AtomicInteger callCount = new AtomicInteger(0);

    // Switch to ACTIVE state.
    Dependency.provideInstance(TestClass.class, new TestClass());
    Dependency.get(TestClass.class);

    BiConsumer<Object, Object> subscriber = (oldA, newA) -> callCount.incrementAndGet();
    Dependency.subscribe(TestClass.class, subscriber);
    Assert.assertEquals(0, callCount.get());

    Dependency.provideInstance(TestClass.class, new TestClass());
    Assert.assertEquals(1, callCount.get());

    Dependency.provideInstance(TestClass.class, new TestClass());
    Assert.assertEquals(2, callCount.get());

    Dependency.unsubscribe(TestClass.class, subscriber);

    // No longer incrementing.
    Dependency.provideInstance(TestClass.class, new TestClass());
    Assert.assertEquals(2, callCount.get());
  }

  @Test
  public void testSubscribeUnsubscribeByName() {
    AtomicInteger callCount = new AtomicInteger(0);
    String instanceName = "ClassA";

    // Switch to ACTIVE state.
    Dependency.provideInstance(TestClass.class, new TestClass());
    Dependency.get(TestClass.class);

    BiConsumer<Object, Object> subscriber = (oldA, newA) -> callCount.incrementAndGet();
    Dependency.subscribe(instanceName, subscriber);
    Assert.assertEquals(0, callCount.get());

    Dependency.provideInstance(instanceName, new TestClass());
    Assert.assertEquals(1, callCount.get());

    Dependency.provideInstance(instanceName, new TestClass());
    Assert.assertEquals(2, callCount.get());

    Dependency.unsubscribe(instanceName, subscriber);

    // No longer incrementing.
    Dependency.provideInstance(instanceName, new TestClass());
    Assert.assertEquals(2, callCount.get());
  }
}
