package com.github.ambry.dependency;

import com.github.ambry.utils.TestUtils;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import org.junit.Test;
import org.junit.Assert;

public class DependencyStoreTest {

  // Declare some test interfaces and classes.
  interface TestInterfaceA {}

  interface TestInterfaceB {}

  @DependencyProvider(instanceClass = TestClassC.class)
  interface TestInterfaceC {}

  static class TestClassA implements TestInterfaceA {}

  @DependencyProvider(instanceClass = TestClassC.class)
  static abstract class TestAbstractClassC implements TestInterfaceC {}

  static class TestClassC extends TestAbstractClassC {}

  @DependencyProvider
  static class TestClassD {}

  static class TestClassAB implements TestInterfaceA, TestInterfaceB {}

  static class TestSubclassAB extends TestClassAB {}

  static class TestSubclassABC extends TestClassAB implements TestInterfaceC {}

  // The testInterface cannot be instantiated because it is an interface, not a class.
  @DependencyProvider(instanceClass = TestInterfaceA.class)
  interface TestBadInstanceClass {}

  // The TestClass does not implement this interface.
  @DependencyProvider(instanceClass = TestClassA.class)
  interface TestBadInstanceClass2 {}

  // The instanceClass is missing.
  @DependencyProvider
  interface TestBadInstanceClass3 {}

  // The TestBadInstanceClass4 is abstract.
  @DependencyProvider
  static abstract class TestBadInstanceClass4 {}

  // The constructor throws.
  @DependencyProvider
  static class TestBadInstanceClass5 {
    public TestBadInstanceClass5() {
      throw new IllegalArgumentException("test");
    }
  }

  @DependencyProvider
  static class TestClassConstructor {
    @DependencyConstructor
    public TestClassConstructor(TestClassD dInstance) {}
  }

  @DependencyProvider
  static class TestNameConstructor {
    @DependencyConstructor
    public TestNameConstructor(@DependencyName("TestClassDName") TestClassD dInstance) {}
  }

  @DependencyProvider
  static class TestMixConstructor {
    @DependencyConstructor
    public TestMixConstructor(TestClassD dInstance1, @DependencyName("TestClassDName") TestClassD dInstance2) {}
  }

  @DependencyProvider
  static class TestIntegerConstructor {
    @DependencyConstructor
    public TestIntegerConstructor(@DependencyName("IntValue") Integer namedValueAllowed) {}
  }

  @DependencyProvider
  static class TestNamedIntConstructor {
    @DependencyConstructor
    public TestNamedIntConstructor(@DependencyName("IntValue") int namedValueAllowed) {}
  }

  @DependencyProvider
  static class TestUnnamedConstructor {
    @DependencyConstructor
    public TestUnnamedConstructor(int primitiveTypeNotAllowed) {}
  }

  @Test
  public void testDependencyProviderAnnotation() {
    DependencyStore dependency = new DependencyStore();

    Assert.assertTrue(dependency.get(TestClassD.class) instanceof TestClassD);
    Assert.assertTrue(dependency.get(TestInterfaceC.class) instanceof TestClassC);
    Assert.assertTrue(dependency.get(TestClassConstructor.class) instanceof TestClassConstructor);

    dependency.provideInstance("TestClassDName", new TestClassD());
    Assert.assertTrue(dependency.get(TestNameConstructor.class) instanceof TestNameConstructor);
    Assert.assertTrue(dependency.get(TestMixConstructor.class) instanceof TestMixConstructor);

    // Test a constructor that takes a named object.
    dependency.provideInstance("IntValue", 10);
    Assert.assertTrue(dependency.get(TestIntegerConstructor.class) instanceof TestIntegerConstructor);
    Assert.assertTrue(dependency.get(TestNamedIntConstructor.class) instanceof TestNamedIntConstructor);
  }

  @Test
  public void testDependencyProviderAnnotationFailed() {
    DependencyStore dependency = new DependencyStore();

    // Did not provide TestClassDName, it would fail.
    Exception ex = TestUtils.getException(() -> dependency.get(TestNameConstructor.class));
    Assert.assertTrue(ex instanceof DependencyException);

    // Primitive parameter type not allowed.
    ex = TestUtils.getException(() -> dependency.get(TestUnnamedConstructor.class));
    Assert.assertTrue(ex instanceof DependencyException);
  }

  @Test
  public void testActivate() throws DependencyNotFoundException {
    DependencyStore dependency = new DependencyStore();

    Assert.assertFalse(dependency.isActivated());
    Assert.assertTrue(dependency.get(TestClassD.class) instanceof TestClassD);
    Assert.assertFalse(dependency.isActivated());

    // Test auto-switch set to FALSE.
    dependency.activate();
    Assert.assertTrue(dependency.isActivated());
    Assert.assertTrue(dependency.get(TestClassD.class) instanceof TestClassD);
  }

  @Test
  public void testProvideInstance() throws DependencyNotFoundException {
    DependencyStore dependency = new DependencyStore();

    // Error parameters cases
    Exception ex = TestUtils.getException(() -> dependency.provideInstance(null, new TestClassA()));
    Assert.assertTrue(ex instanceof NullPointerException);

    ex = TestUtils.getException(() -> dependency.provideInstance(TestInterfaceA.class, null));
    Assert.assertTrue(ex instanceof NullPointerException);

    // Normal cases.
    dependency.provideInstance(TestInterfaceA.class, new TestClassA());
    dependency.provideInstance(TestInterfaceB.class, new TestSubclassAB());

    Assert.assertEquals(TestClassA.class, dependency.get(TestInterfaceA.class).getClass());
    Assert.assertEquals(TestSubclassAB.class, dependency.get(TestInterfaceB.class).getClass());

    // TestClass is never published, tryGet() will return null.
    Assert.assertNull(dependency.tryGet(TestClassA.class));
  }

  @Test
  public void testProvideSupplier_InSetupState() throws DependencyNotFoundException {
    DependencyStore dependency = new DependencyStore();

    // Error parameters cases
    Exception ex = TestUtils.getException(() -> dependency.provideSupplier(null, TestClassA::new));
    Assert.assertTrue(ex instanceof NullPointerException);

    ex = TestUtils.getException(() -> dependency.provideSupplier(TestInterfaceA.class, null));
    Assert.assertTrue(ex instanceof NullPointerException);

    // Normal cases.
    AtomicInteger testClassACount = new AtomicInteger(0);
    AtomicInteger testSubclassABCount = new AtomicInteger(0);
    dependency.provideSupplier(TestInterfaceA.class, () -> { testClassACount.getAndAdd(1); return new TestClassA(); });
    dependency.provideSupplier(TestInterfaceB.class, () -> { testSubclassABCount.getAndAdd(1); return new TestSubclassAB(); });

    dependency.activate();

    Assert.assertEquals(0, testClassACount.get());
    Assert.assertEquals(TestClassA.class, dependency.get(TestInterfaceA.class).getClass());
    Assert.assertEquals(1, testClassACount.get());
    Assert.assertEquals(TestClassA.class, dependency.tryGet(TestInterfaceA.class).getClass());
    Assert.assertEquals(1, testClassACount.get());

    Assert.assertEquals(0, testSubclassABCount.get());
    Assert.assertEquals(TestSubclassAB.class, dependency.get(TestInterfaceB.class).getClass());
    Assert.assertEquals(1, testSubclassABCount.get());
    Assert.assertEquals(TestSubclassAB.class, dependency.get(TestInterfaceB.class).getClass());
    Assert.assertEquals(1, testSubclassABCount.get());

    // Test an error case.  In Active state, call a supplier that throws.
    ex = TestUtils.getException(() -> dependency.provideSupplier(TestInterfaceA.class,
        () -> { throw new IllegalArgumentException("bad"); }));
    Assert.assertTrue(ex instanceof DependencyException);
  }

  @Test
  public void testProvideSupplier_InActiveState() throws DependencyNotFoundException {
    DependencyStore dependency = new DependencyStore();

    AtomicInteger testClassACount = new AtomicInteger(0);
    AtomicInteger testSubclassABCount = new AtomicInteger(0);

    dependency.activate();

    Assert.assertEquals(0, testClassACount.get());
    Assert.assertEquals(0, testSubclassABCount.get());

    dependency.provideSupplier(TestInterfaceA.class, () -> { testClassACount.getAndAdd(1); return new TestClassA(); });
    dependency.provideSupplier(TestInterfaceB.class, () -> { testSubclassABCount.getAndAdd(1); return new TestSubclassAB(); });

    Assert.assertEquals(1, testClassACount.get());
    Assert.assertEquals(TestClassA.class, dependency.tryGet(TestInterfaceA.class).getClass());
    Assert.assertEquals(1, testClassACount.get());

    Assert.assertEquals(1, testSubclassABCount.get());
    Assert.assertEquals(TestSubclassAB.class, dependency.get(TestInterfaceB.class).getClass());
    Assert.assertEquals(1, testSubclassABCount.get());
  }

  @Test
  public void testProvideFactory() throws DependencyNotFoundException {
    DependencyStore dependency = new DependencyStore();

    // Error parameters cases
    Exception ex = TestUtils.getException(() -> dependency.provideFactory(null, TestClassA::new));
    Assert.assertTrue(ex instanceof NullPointerException);

    ex = TestUtils.getException(() -> dependency.provideFactory(TestInterfaceA.class, null));
    Assert.assertTrue(ex instanceof NullPointerException);

    // Normal cases.
    AtomicInteger testClassACount = new AtomicInteger(0);
    AtomicInteger testSubclassABCount = new AtomicInteger(0);
    dependency.provideFactory(TestInterfaceA.class, () -> { testClassACount.getAndAdd(1); return new TestClassA(); });
    dependency.provideFactory(TestInterfaceB.class, () -> { testSubclassABCount.getAndAdd(1); return new TestSubclassAB(); });

    Assert.assertEquals(0, testClassACount.get());
    Assert.assertEquals(TestClassA.class, dependency.get(TestInterfaceA.class).getClass());
    Assert.assertEquals(1, testClassACount.get());
    Assert.assertEquals(TestClassA.class, dependency.tryGet(TestInterfaceA.class).getClass());
    Assert.assertEquals(2, testClassACount.get());

    Assert.assertEquals(0, testSubclassABCount.get());
    Assert.assertEquals(TestSubclassAB.class, dependency.get(TestInterfaceB.class).getClass());
    Assert.assertEquals(1, testSubclassABCount.get());
    Assert.assertEquals(TestSubclassAB.class, dependency.get(TestInterfaceB.class).getClass());
    Assert.assertEquals(2, testSubclassABCount.get());
  }

  @Test
  public void testHasProvider() {
    DependencyStore dependency = new DependencyStore();

    // Test error parameter case.
    Exception ex = TestUtils.getException(() -> dependency.hasProvider(null));
    Assert.assertTrue(ex instanceof NullPointerException);

    // Normal test.
    dependency.provideInstance(TestInterfaceA.class, new TestClassA());
    dependency.provideSupplier(TestInterfaceB.class, TestSubclassAB::new);
    dependency.provideFactory(TestClassAB.class, TestClassAB::new);

    Assert.assertTrue(dependency.hasProvider(TestInterfaceA.class));
    Assert.assertTrue(dependency.hasProvider(TestInterfaceB.class));
    Assert.assertTrue(dependency.hasProvider(TestClassAB.class));

    // These provider uses DependencyProvider annotation.
    Assert.assertTrue(dependency.hasProvider(TestInterfaceC.class));
    Assert.assertTrue(dependency.hasProvider(TestAbstractClassC.class));
    Assert.assertTrue(dependency.hasProvider(TestBadInstanceClass.class));
    Assert.assertTrue(dependency.hasProvider(TestBadInstanceClass4.class));
  }

  @Test
  public void testGet_InvalidDependencyProviderAnnotations() {
    DependencyStore dependency = new DependencyStore();
    // Error cases.  These @DependencyProvider has errors.
    // The testInterface cannot be instantiated because it is an interface, not a class.
    Exception ex = TestUtils.getException(()-> dependency.get(TestBadInstanceClass.class));
    Assert.assertTrue(ex instanceof DependencyException);

    // The TestClass does not implement this interface.
    ex = TestUtils.getException(()-> dependency.get(TestBadInstanceClass2.class));
    Assert.assertTrue(ex instanceof DependencyException);

    // The instanceClass is missing.
    ex = TestUtils.getException(()-> dependency.get(TestBadInstanceClass3.class));
    Assert.assertTrue(ex instanceof DependencyException);

    // The TestBadInstanceClass4 is abstract.
    ex = TestUtils.getException(()-> dependency.get(TestBadInstanceClass4.class));
    Assert.assertTrue(ex instanceof DependencyException);

    // Test constructor that throws.
    ex = TestUtils.getException(()-> dependency.get(TestBadInstanceClass5.class));
    Assert.assertTrue(ex instanceof DependencyException);

    // Get something that doesn't exist.
    ex = TestUtils.getException(()-> dependency.get(TestClassA.class));
    Assert.assertTrue(ex instanceof DependencyNotFoundException);
  }

  @Test
  public void testGet_ValidDependencyProviderAnnotations() throws DependencyNotFoundException {
    DependencyStore dependency = new DependencyStore();

    //DependencyProvider(instanceClass = TestClassC.class)
    //abstract class TestAbstractClassC implements TestInterfaceC {}
    Assert.assertTrue(dependency.get(TestAbstractClassC.class) instanceof TestClassC);

    //DependencyProvider(instanceClass = TestClassC.class)
    //interface TestInterfaceC {}
    Assert.assertTrue(dependency.get(TestInterfaceC.class) instanceof TestClassC);

    //DependencyProvider
    //class TestClassD {}
    Assert.assertNotNull(dependency.get(TestClassD.class));
  }

  @Test
  public void testTryGet() {
    DependencyStore dependency = new DependencyStore();

    // Test error parameter case.
    Exception ex = TestUtils.getException(() -> dependency.tryGet(null));
    Assert.assertTrue(ex instanceof NullPointerException);

    // Normal
    dependency.provideInstance(TestClassA.class, new TestClassA());
    dependency.provideSupplier(TestInterfaceB.class, TestClassAB::new);
    dependency.provideFactory(TestInterfaceC.class, TestClassC::new);

    Assert.assertTrue(dependency.tryGet(TestClassA.class) instanceof TestClassA);
    Assert.assertTrue(dependency.tryGet(TestInterfaceB.class) instanceof TestClassAB);
    Assert.assertTrue(dependency.tryGet(TestInterfaceC.class) instanceof TestClassC);
    Assert.assertTrue(dependency.tryGet(TestClassD.class) instanceof TestClassD);

    // Try to get something that doesn't exist.
    Assert.assertNull(dependency.tryGet(TestSubclassAB.class));
  }

  @Test
  public void testRemove() {
    DependencyStore dependency = new DependencyStore();

    // Test error parameter case.
    Exception ex = TestUtils.getException(() -> dependency.remove(null));
    Assert.assertTrue(ex instanceof NullPointerException);

    // Test normal case.
    dependency.provideInstance(TestClassA.class, new TestClassA());
    dependency.provideSupplier(TestInterfaceB.class, TestClassAB::new);
    dependency.provideFactory(TestInterfaceC.class, TestSubclassABC::new);

    dependency.remove(TestClassA.class);
    dependency.remove(TestInterfaceB.class);
    dependency.remove(TestInterfaceC.class);

    // None of them should exist.
    Assert.assertNull(dependency.tryGet(TestClassA.class));
    Assert.assertNull(dependency.tryGet(TestInterfaceB.class));

    // TestInterfaceC has DependencyProvider annotation.
    Assert.assertTrue(dependency.tryGet(TestInterfaceC.class) instanceof TestClassC);
    Assert.assertTrue(dependency.tryGet(TestClassD.class) instanceof TestClassD);
  }

  @Test
  public void testSubscribe_InSetupState() {
    DependencyStore dependency = new DependencyStore();
    AtomicInteger callCount1 = new AtomicInteger(0);
    AtomicReference<Object> oldValue1 = new AtomicReference<>(null);
    AtomicReference<Object> newValue1 = new AtomicReference<>(null);

    AtomicInteger callCount2 = new AtomicInteger(0);
    AtomicReference<Object> oldValue2 = new AtomicReference<>(null);
    AtomicReference<Object> newValue2 = new AtomicReference<>(null);

    BiConsumer<Object, Object> subscriberA1 =
        (oldA, newA) -> { callCount1.incrementAndGet(); oldValue1.set(oldA); newValue1.set(newA); };
    BiConsumer<Object, Object> subscriberA2 =
        (oldA, newA) -> { callCount2.incrementAndGet(); oldValue2.set(oldA); newValue2.set(newA); };

    // Subscribe during SETUP state.
    dependency.subscribe(TestInterfaceA.class, subscriberA1);
    dependency.subscribe(TestInterfaceA.class, subscriberA2);

    // Provide the instance.
    TestClassA newInstanceA = new TestClassA();
    dependency.provideInstance(TestInterfaceA.class, newInstanceA);

    Assert.assertEquals(0, callCount1.get());
    Assert.assertEquals(0, callCount2.get());

    dependency.activate();

    Assert.assertEquals(1, callCount1.get());
    Assert.assertEquals(1, callCount2.get());
    Assert.assertNull(oldValue1.get());
    Assert.assertNull(oldValue2.get());
    Assert.assertEquals(newInstanceA, newValue1.get());
    Assert.assertEquals(newInstanceA, newValue2.get());
  }

  @Test
  public void testSubscribe_InActiveState() {
    DependencyStore dependency = new DependencyStore();
    AtomicInteger callCount1 = new AtomicInteger(0);
    AtomicReference<Object> oldValue1 = new AtomicReference<>(null);
    AtomicReference<Object> newValue1 = new AtomicReference<>(null);

    AtomicInteger callCount2 = new AtomicInteger(0);
    AtomicReference<Object> oldValue2 = new AtomicReference<>(null);
    AtomicReference<Object> newValue2 = new AtomicReference<>(null);

    BiConsumer<Object, Object> subscriberA1 =
        (oldA, newA) -> { callCount1.incrementAndGet(); oldValue1.set(oldA); newValue1.set(newA); };
    BiConsumer<Object, Object> subscriberA2 =
        (oldA, newA) -> { callCount2.incrementAndGet(); oldValue2.set(oldA); newValue2.set(newA); };

    // Set provider for TestInterfaceA.
    TestClassA oldInstanceA = new TestClassA();
    dependency.provideInstance(TestInterfaceA.class, oldInstanceA);

    dependency.activate();

    // Subscribe during ACTIVE state.
    dependency.subscribe(TestInterfaceA.class, subscriberA1);
    dependency.subscribe(TestInterfaceA.class, subscriberA2);

    Assert.assertEquals(0, callCount1.get());
    Assert.assertEquals(0, callCount2.get());

    // Change TestInterfaceA provider.
    TestClassA newInstanceA = new TestClassA();
    dependency.provideInstance(TestInterfaceA.class, newInstanceA);

    Assert.assertEquals(1, callCount1.get());
    Assert.assertEquals(1, callCount2.get());
    Assert.assertEquals(oldInstanceA, oldValue1.get());
    Assert.assertEquals(oldInstanceA, oldValue2.get());
    Assert.assertEquals(newInstanceA, newValue1.get());
    Assert.assertEquals(newInstanceA, newValue2.get());

    // Remove() will call subscribers too.
    dependency.remove(TestInterfaceA.class);
    Assert.assertEquals(2, callCount1.get());
    Assert.assertEquals(2, callCount2.get());
    Assert.assertEquals(newInstanceA, oldValue1.get());
    Assert.assertEquals(newInstanceA, oldValue2.get());
    Assert.assertNull(newValue1.get());
    Assert.assertNull(newValue2.get());
  }

  @Test
  public void testSubscribe_SubscriberThrows() {
    DependencyStore dependency = new DependencyStore();

    dependency.subscribe(TestInterfaceA.class, (oldA, newA) -> { throw new IllegalArgumentException("fest"); });

    dependency.activate();

    // Set provider for TestInterfaceA.  Subscriber throws.
    Exception ex = TestUtils.getException(() -> dependency.provideInstance(TestInterfaceA.class, new TestClassA()));
    Assert.assertTrue(ex instanceof DependencyException);
  }

  @Test
  public void testUnsubscribe() {
    DependencyStore dependency = new DependencyStore();
    AtomicInteger callCount1 = new AtomicInteger(0);
    AtomicReference<Object> oldValue1 = new AtomicReference<>(null);
    AtomicReference<Object> newValue1 = new AtomicReference<>(null);

    AtomicInteger callCount2 = new AtomicInteger(0);
    AtomicReference<Object> oldValue2 = new AtomicReference<>(null);
    AtomicReference<Object> newValue2 = new AtomicReference<>(null);

    BiConsumer<Object, Object> subscriberA1 =
        (oldA, newA) -> { callCount1.incrementAndGet(); oldValue1.set(oldA); newValue1.set(newA); };
    BiConsumer<Object, Object> subscriberA2 =
        (oldA, newA) -> { callCount2.incrementAndGet(); oldValue2.set(oldA); newValue2.set(newA); };

    // Set TestInterfaceA provider.
    TestClassA oldInstanceA = new TestClassA();
    dependency.provideInstance(TestInterfaceA.class, oldInstanceA);

    // Subscribe during SETUP state.
    dependency.subscribe(TestInterfaceA.class, subscriberA1);
    dependency.subscribe(TestInterfaceA.class, subscriberA2);

    dependency.activate();

    Assert.assertEquals(1, callCount1.get());
    Assert.assertEquals(1, callCount2.get());
    Assert.assertNull(oldValue1.get());
    Assert.assertNull(oldValue2.get());
    Assert.assertEquals(oldInstanceA, newValue1.get());
    Assert.assertEquals(oldInstanceA, newValue2.get());

    // Change TestInterfaceA provider.
    TestClassA newInstanceA = new TestClassA();
    dependency.provideInstance(TestInterfaceA.class, newInstanceA);

    Assert.assertEquals(2, callCount1.get());
    Assert.assertEquals(2, callCount2.get());
    Assert.assertEquals(oldInstanceA, oldValue1.get());
    Assert.assertEquals(oldInstanceA, oldValue2.get());
    Assert.assertEquals(newInstanceA, newValue1.get());
    Assert.assertEquals(newInstanceA, newValue2.get());

    // Remove one subscriber.
    dependency.unsubscribe(TestInterfaceA.class, subscriberA1);

    // Change TestInterfaceA provider.
    TestClassA newInstanceA2 = new TestClassA();
    dependency.provideInstance(TestInterfaceA.class, newInstanceA2);

    Assert.assertEquals(2, callCount1.get());
    Assert.assertEquals(3, callCount2.get());
    Assert.assertEquals(oldInstanceA, oldValue1.get());
    Assert.assertEquals(newInstanceA, oldValue2.get());
    Assert.assertEquals(newInstanceA, newValue1.get());
    Assert.assertEquals(newInstanceA2, newValue2.get());

    // Remove the other subscriber.
    dependency.unsubscribe(TestInterfaceA.class, subscriberA2);

    // Change TestInterfaceA provider.  Should have no effect since there is no subscribers.
    TestClassA newInstanceA3 = new TestClassA();
    dependency.provideInstance(TestInterfaceA.class, newInstanceA3);

    // The call count remains unchanged.  Values remain unchanged.
    Assert.assertEquals(2, callCount1.get());
    Assert.assertEquals(3, callCount2.get());
    Assert.assertEquals(oldInstanceA, oldValue1.get());
    Assert.assertEquals(newInstanceA, oldValue2.get());
    Assert.assertEquals(newInstanceA, newValue1.get());
    Assert.assertEquals(newInstanceA2, newValue2.get());
  }
}