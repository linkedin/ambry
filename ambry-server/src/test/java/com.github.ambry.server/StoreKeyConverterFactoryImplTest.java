package com.github.ambry.server;

import com.github.ambry.store.StoreKeyConverter;
import com.github.ambry.store.StoreKeyConverterFactory;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Test for {@link StoreKeyConverterFactoryImpl}
 */
public class StoreKeyConverterFactoryImplTest {

  /**
   * Ensures StoreKeyConverterFactoryImpl returns {@link StoreKeyConverterImplNoOp}
   * @throws InstantiationException
   */
  @Test
  public void basicTest() throws InstantiationException {
    StoreKeyConverterFactory storeKeyConverterFactory = new StoreKeyConverterFactoryImpl();
    StoreKeyConverter storeKeyConverter = storeKeyConverterFactory.getStoreKeyConverter();
    assertTrue(storeKeyConverter instanceof StoreKeyConverterImplNoOp);
  }
}
