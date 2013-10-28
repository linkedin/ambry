package com.github.ambry;

import java.net.SocketAddress;
import java.io.InputStream;

/**
 * Simple request
 */
public interface Request {

  InputStream getInputStream();
}
