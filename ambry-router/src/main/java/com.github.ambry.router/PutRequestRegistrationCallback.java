package com.github.ambry.router;

import com.github.ambry.network.RequestInfo;


/**
 * The callback to be used when put requests are created and needs to be sent out. The {@link PutManager} passes this
 * callback to the {@link PutOperation} and the {@link PutOperation} uses this callback when requests are created and
 * need to be sent out.
 */
public interface PutRequestRegistrationCallback {
  public void registerRequestToSend(PutOperation putOperation, RequestInfo request);
}
