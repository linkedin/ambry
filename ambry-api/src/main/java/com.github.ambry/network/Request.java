package com.github.ambry.network;

import java.io.InputStream;

/**
 * Simple request
 */
public interface Request {

  InputStream getInputStream();
}
