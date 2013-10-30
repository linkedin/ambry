
package com.github.ambry.store;

import com.github.ambry.store.MessageReadSet;
import com.github.ambry.store.StoreException;

import java.io.OutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.io.IOException;

/**
 * The object store's underlying store
 */
public interface Store {

    void start() throws StoreException;

    MessageReadSet get(ArrayList<String> handles) throws StoreException;

    void put (String handle, InputStream value) throws StoreException;

    void delete(ArrayList<String> handles) throws StoreException;

    void shutdown();
}
