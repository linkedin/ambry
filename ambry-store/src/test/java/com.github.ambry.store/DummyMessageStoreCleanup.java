package com.github.ambry.store;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class DummyMessageStoreCleanup implements MessageStoreCleanup {

  @Override
  public List<ReplaceInfo> getReplacementInfo(MessageReadSet readSet, StoreKeyFactory factory)
      throws IOException  {
    return new ArrayList<ReplaceInfo>();
  }
}

