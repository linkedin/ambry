package com.github.ambry.store;

import java.nio.ByteBuffer;

/**
 * Created with IntelliJ IDEA.
 * User: srsubram
 * Date: 10/25/13
 * Time: 4:37 PM
 * To change this template use File | Settings | File Templates.
 */
public interface IndexKey {
  ByteBuffer toBytes();
}
