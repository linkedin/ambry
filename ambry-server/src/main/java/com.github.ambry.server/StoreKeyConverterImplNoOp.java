package com.github.ambry.server;

import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreKeyConverter;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;


/**
 * No-op StoreKeyConverter. StoreKeys get paired with themselves
 */
class StoreKeyConverterImplNoOp implements StoreKeyConverter {

  @Override
  public Map<StoreKey, StoreKey> convert(Collection<StoreKey> input) throws Exception {
    Map<StoreKey, StoreKey> output = new HashMap<>();
    if (input != null) {
      input.forEach((storeKey) -> output.put(storeKey, storeKey));
    }
    return output;
  }
}
